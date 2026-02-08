package grpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/port/remotechunknode"
	chunkv1 "github.com/amari/mithril/gen/go/proto/mithril/chunk/v1"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ remotechunknode.RemoteChunkClient = (*RemoteChunkClient)(nil)

// RemoteChunkClient implements the RemoteChunkClient port using gRPC.
//
// Architecture:
// - Uses chunk-node:/// gRPC resolver for NodeID → address resolution
// - Caches *grpc.ClientConn per NodeID (gRPC handles lifecycle, reconnection, health)
// - Maps gRPC errors with ErrorDetails to domain RemoteError
// - Implements robust retrying stream reader with offset tracking
//
// Connection lifecycle:
// - Connections are lazy-created on first use
// - Each NodeID gets exactly one *grpc.ClientConn (pooled)
// - The chunk-node:/// resolver watches for node changes (StartupNonce, GRPCURLs)
// - gRPC automatically reconnects on connection failures
// - Active eviction is rare (only on special cases like resolver errors)
//
// Error mapping:
// - Extracts ErrorDetails from gRPC status
// - Constructs RemoteError with remote chunk/volume state
// - Retries on ERROR_CODE_WRONG_NODE (resolution race, node migration)
// - Retries on transient gRPC codes (Unavailable, DeadlineExceeded, ResourceExhausted)
type RemoteChunkClient struct {
	dialOptions []grpc.DialOption
	log         *zerolog.Logger

	// Metrics
	poolHits   metric.Int64Counter
	poolMisses metric.Int64Counter

	mu    sync.RWMutex
	conns map[domain.NodeID]*grpc.ClientConn
}

// NewRemoteChunkClient creates a gRPC-based remote chunk client.
// The dialOptions are shared across all connections (TLS, auth, timeouts, etc.).
//
// Prerequisites:
// - The chunk-node:/// gRPC resolver must be registered before creating connections
// - The resolver needs MemberResolver and MemberWatchManager to be available
//
// Example:
//
//	client, err := grpc.NewRemoteChunkClient(
//	    []grpc.DialOption{
//	        grpc.WithTransportCredentials(creds),
//	        grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`),
//	    },
//	    meter,
//	    log,
//	)
func NewRemoteChunkClient(
	dialOptions []grpc.DialOption,
	meter metric.Meter,
	log *zerolog.Logger,
) (*RemoteChunkClient, error) {
	poolHits, err := meter.Int64Counter("remote_chunk_client.connection_pool.hits")
	if err != nil {
		return nil, fmt.Errorf("failed to create pool hits counter: %w", err)
	}

	poolMisses, err := meter.Int64Counter("remote_chunk_client.connection_pool.misses")
	if err != nil {
		return nil, fmt.Errorf("failed to create pool misses counter: %w", err)
	}

	return &RemoteChunkClient{
		dialOptions: dialOptions,
		log:         log,
		poolHits:    poolHits,
		poolMisses:  poolMisses,
		conns:       make(map[domain.NodeID]*grpc.ClientConn),
	}, nil
}

// ReadChunkRange returns a retrying stream reader that reads from the remote chunk.
//
// The reader is lazy - it doesn't open the stream until the first Read() call.
// This allows the caller to set up the reader, pass it around, and let the
// consumer control when the network request happens.
//
// The reader automatically retries on:
// - ERROR_CODE_WRONG_NODE (re-resolves NodeID and tries again)
// - Transient gRPC errors (Unavailable, DeadlineExceeded, ResourceExhausted)
// - Premature EOF (stream ends before all data received)
//
// On retry, the reader resumes from where it left off (offset = startOffset + bytesRead).
// This ensures we don't re-download data that was already successfully read.
func (c *RemoteChunkClient) ReadChunkRange(
	ctx context.Context,
	chunkID domain.ChunkID,
	offset int64,
	length int64,
) (io.ReadCloser, error) {
	nodeID := chunkID.NodeID()

	return &retryingStreamReader{
		ctx:         ctx,
		client:      c,
		chunkID:     chunkID,
		nodeID:      nodeID,
		startOffset: offset,
		totalLength: length,
		log:         c.log,
	}, nil
}

// StatChunk retrieves metadata for a remote chunk with retries.
//
// Retry logic:
// - Retries on ERROR_CODE_WRONG_NODE (resolution race)
// - Retries on transient gRPC errors
// - Uses retry policy from context (or defaults)
// - Exponential backoff between attempts
//
// Error handling:
// - Returns RemoteError for remote node errors (includes remote state)
// - Returns wrapped error for transport failures
func (c *RemoteChunkClient) StatChunk(
	ctx context.Context,
	chunkID domain.ChunkID,
) (*chunkv1.Chunk, *chunkv1.Volume, error) {
	nodeID := chunkID.NodeID()
	policy := remotechunknode.GetRetryPolicy(ctx)

	var attempt int32
	backoff := policy.InitialInterval

	for {
		attempt++

		conn, err := c.getConnection(ctx, nodeID)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get connection to node %08x: %w", nodeID, err)
		}

		client := chunkv1.NewChunkServiceClient(conn)

		resp, err := client.StatChunk(ctx, &chunkv1.StatChunkRequest{
			ChunkId: chunkID[:],
		})
		if err != nil {
			// Try to extract and wrap remote error
			if err = extractErrorDetails(err); err != nil {
				var remoteErr *remoteError
				if errors.As(err, &remoteErr) {
					// Check if retryable
					if isRetryableError(remoteErr.ErrorCode) {
						c.log.Warn().
							Err(err).
							Uint32("node_id", uint32(nodeID)).
							Int32("attempt", attempt).
							Dur("backoff", backoff).
							Msg("retryable error stating remote chunk")

						if policy.MaximumAttempts > 0 && attempt >= policy.MaximumAttempts {
							return nil, nil, fmt.Errorf("exhausted %d retry attempts: %w", attempt, remoteErr)
						}

						select {
						case <-time.After(backoff):
							backoff = time.Duration(float64(backoff) * policy.BackoffCoefficient)
							if backoff > policy.MaximumInterval {
								backoff = policy.MaximumInterval
							}
							continue
						case <-ctx.Done():
							return nil, nil, fmt.Errorf("context canceled during retry: %w", ctx.Err())
						}
					}
				}

				// Non-retryable remote error
				return nil, nil, err
			}

			// Check for transient gRPC errors
			if isTransientGRPCError(err) {
				c.log.Warn().
					Err(err).
					Uint32("node_id", uint32(nodeID)).
					Int32("attempt", attempt).
					Dur("backoff", backoff).
					Msg("transient gRPC error stating remote chunk")

				if policy.MaximumAttempts > 0 && attempt >= policy.MaximumAttempts {
					return nil, nil, fmt.Errorf("exhausted %d retry attempts: %w", attempt, err)
				}

				select {
				case <-time.After(backoff):
					backoff = time.Duration(float64(backoff) * policy.BackoffCoefficient)
					if backoff > policy.MaximumInterval {
						backoff = policy.MaximumInterval
					}
					continue
				case <-ctx.Done():
					return nil, nil, fmt.Errorf("context canceled during retry: %w", ctx.Err())
				}
			}

			// Non-retryable error
			return nil, nil, fmt.Errorf("non-retryable error: %w", err)
		}

		return resp.Chunk, resp.Volume, nil
	}
}

// getConnection returns a gRPC connection to the specified node, using connection pooling.
//
// Connection pooling:
// - One *grpc.ClientConn per NodeID (shared across all operations)
// - Connections are cached until Close() is called
// - gRPC handles reconnection, load balancing, health checking
//
// Resolution:
// - Uses chunk-node:///{nodeID} target
// - The chunk-node resolver uses MemberResolver for initial lookup
// - The chunk-node resolver uses MemberWatchManager to detect node changes
// - Automatic re-resolution when StartupNonce changes (node restart)
func (c *RemoteChunkClient) getConnection(ctx context.Context, nodeID domain.NodeID) (*grpc.ClientConn, error) {
	// Check pool (fast path)
	c.mu.RLock()
	conn, exists := c.conns[nodeID]
	c.mu.RUnlock()

	if exists {
		c.poolHits.Add(ctx, 1)
		return conn, nil
	}

	// Pool miss - need to dial
	c.poolMisses.Add(ctx, 1)

	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check after acquiring write lock
	if conn, exists := c.conns[nodeID]; exists {
		return conn, nil
	}

	// Dial new connection using chunk-node resolver
	// The chunk-node:/// resolver handles:
	// - MemberResolver for initial resolution
	// - MemberWatchManager for tracking node changes
	// - Automatic reconnection on StartupNonce change
	// - Priority-ordered GRPCURLs fallback
	target := fmt.Sprintf("chunk-node:///%08x", nodeID)

	c.log.Debug().
		Uint32("node_id", uint32(nodeID)).
		Str("target", target).
		Msg("dialing new connection to remote chunk node")

	conn, err := grpc.NewClient(target, c.dialOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to create client for node %08x: %w", nodeID, err)
	}

	// Store in pool
	c.conns[nodeID] = conn

	c.log.Debug().
		Uint32("node_id", uint32(nodeID)).
		Msg("established new gRPC connection to remote chunk node")

	return conn, nil
}

// startStream initiates a ReadChunk stream and validates the header.
//
// Header validation:
// - First message MUST be a header (not data)
// - Header MUST contain total_data_length
// - total_data_length MUST match requested length
//
// This validation ensures the remote node understood our request correctly
// and will send exactly the amount of data we expect.
func (c *RemoteChunkClient) startStream(
	ctx context.Context,
	nodeID domain.NodeID,
	chunkID domain.ChunkID,
	offset int64,
	length int64,
) (chunkv1.ChunkService_ReadChunkClient, int64, error) {
	conn, err := c.getConnection(ctx, nodeID)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get connection to node %08x: %w", nodeID, err)
	}

	client := chunkv1.NewChunkServiceClient(conn)

	stream, err := client.ReadChunk(ctx, &chunkv1.ReadChunkRequest{
		ChunkId: chunkID[:],
		Offset:  offset,
		Length:  length,
	})
	if err != nil {
		// Try to extract remote error
		if remoteErr := extractErrorDetails(err); remoteErr != nil {
			return nil, 0, remoteErr
		}
		return nil, 0, err
	}

	// Receive and validate header
	msg, err := stream.Recv()
	if err != nil {
		// Try to extract remote error
		if remoteErr := extractErrorDetails(err); remoteErr != nil {
			return nil, 0, remoteErr
		}
		return nil, 0, fmt.Errorf("failed to receive header: %w", err)
	}

	header := msg.GetHeader()
	if header == nil {
		return nil, 0, fmt.Errorf("expected header as first message, got data")
	}

	// Validate total data length matches request
	if header.TotalDataLength != length {
		return nil, 0, fmt.Errorf("header total_data_length (%d) does not match requested length (%d)", header.TotalDataLength, length)
	}

	return stream, header.TotalDataLength, nil
}

// Close closes all pooled connections.
// Should be called during graceful shutdown.
func (c *RemoteChunkClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var firstErr error

	for nodeID, conn := range c.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(c.conns, nodeID)

		c.log.Debug().
			Uint32("node_id", uint32(nodeID)).
			Msg("closed connection to remote chunk node")
	}

	c.log.Info().Msg("closed all remote chunk node connections")

	return firstErr
}

// retryingStreamReader implements io.ReadCloser with automatic retries.
// Maintains read offset to resume from the correct position on retry.
//
// Behavior:
// - Lazily starts the stream on first Read()
// - Validates header on stream start (checks total_data_length)
// - Buffers data from each stream message
// - Retries on transient errors or ERROR_CODE_WRONG_NODE
// - Preserves offset: on retry, requests remaining data (totalLength - bytesRead)
// - Detects premature EOF (stream ends before all data received)
// - Detects excess data (stream sends more than requested)
//
// Offset tracking:
// - startOffset: original request offset (never changes)
// - bytesRead: cumulative bytes successfully read
// - Current offset on retry: startOffset + bytesRead
// - Remaining length on retry: totalLength - bytesRead
//
// Example flow:
// 1. Request: offset=1000, length=500
// 2. Read 200 bytes successfully
// 3. Connection drops
// 4. Retry: offset=1200 (1000+200), length=300 (500-200)
// 5. Read remaining 300 bytes
// 6. Return io.EOF (all 500 bytes received)
type retryingStreamReader struct {
	ctx         context.Context
	client      *RemoteChunkClient
	chunkID     domain.ChunkID
	nodeID      domain.NodeID
	startOffset int64 // Original request offset
	totalLength int64 // Total length requested
	log         *zerolog.Logger

	stream         chunkv1.ChunkService_ReadChunkClient
	expectedTotal  int64 // Total data length from header
	bytesRead      int64 // Bytes successfully read so far
	buffer         []byte
	bufferPos      int
	closed         bool
	headerReceived bool
	attempt        int32
	backoff        time.Duration
}

func (r *retryingStreamReader) Read(p []byte) (n int, err error) {
	if r.closed {
		return 0, io.EOF
	}

	policy := remotechunknode.GetRetryPolicy(r.ctx)

	if r.backoff == 0 {
		r.backoff = policy.InitialInterval
	}

	for {
		// Initialize stream if needed
		if r.stream == nil {
			r.attempt++

			if policy.MaximumAttempts > 0 && r.attempt > policy.MaximumAttempts {
				return 0, fmt.Errorf("exhausted %d retry attempts", policy.MaximumAttempts)
			}

			// Calculate current offset (start + bytes already read)
			currentOffset := r.startOffset + r.bytesRead
			remainingLength := r.totalLength - r.bytesRead

			r.log.Debug().
				Int32("attempt", r.attempt).
				Int64("offset", currentOffset).
				Int64("length", remainingLength).
				Int64("bytes_read", r.bytesRead).
				Msg("starting/retrying remote chunk read stream")

			stream, expectedTotal, err := r.client.startStream(r.ctx, r.nodeID, r.chunkID, currentOffset, remainingLength)
			if err != nil {
				// Check if error is retryable
				var remoteErr *remoteError
				if errors.As(err, &remoteErr) {
					if isRetryableError(remoteErr.ErrorCode) {
						r.log.Warn().
							Err(err).
							Int32("attempt", r.attempt).
							Dur("backoff", r.backoff).
							Msg("retryable error starting stream")

						select {
						case <-time.After(r.backoff):
							r.backoff = time.Duration(float64(r.backoff) * policy.BackoffCoefficient)
							if r.backoff > policy.MaximumInterval {
								r.backoff = policy.MaximumInterval
							}
							continue
						case <-r.ctx.Done():
							return 0, r.ctx.Err()
						}
					}
				}

				if isTransientGRPCError(err) {
					r.log.Warn().
						Err(err).
						Int32("attempt", r.attempt).
						Dur("backoff", r.backoff).
						Msg("transient gRPC error starting stream")

					select {
					case <-time.After(r.backoff):
						r.backoff = time.Duration(float64(r.backoff) * policy.BackoffCoefficient)
						if r.backoff > policy.MaximumInterval {
							r.backoff = policy.MaximumInterval
						}
						continue
					case <-r.ctx.Done():
						return 0, r.ctx.Err()
					}
				}

				// Non-retryable error
				return 0, err
			}

			r.stream = stream
			r.expectedTotal = expectedTotal
			r.headerReceived = true
			r.buffer = nil
			r.bufferPos = 0
		}

		// Serve from buffer if we have data
		if r.bufferPos < len(r.buffer) {
			copied := copy(p, r.buffer[r.bufferPos:])
			r.bufferPos += copied
			r.bytesRead += int64(copied)
			return copied, nil
		}

		// Buffer exhausted, get next message
		msg, err := r.stream.Recv()
		if err != nil {
			if err == io.EOF {
				// Check if we received all expected data
				if r.bytesRead < r.totalLength {
					// Premature EOF - retry
					r.log.Warn().
						Int64("bytes_read", r.bytesRead).
						Int64("expected", r.totalLength).
						Msg("premature EOF, retrying")
					r.stream = nil
					continue
				}
				// Got all data, clean EOF
				r.closed = true
				return 0, io.EOF
			}

			// Check if error is retryable
			var remoteErr *remoteError
			if errors.As(err, &remoteErr) {
				if isRetryableError(remoteErr.ErrorCode) {
					r.log.Warn().
						Err(err).
						Int64("bytes_read", r.bytesRead).
						Msg("retryable error receiving, retrying from current position")
					r.stream = nil
					continue
				}
			}

			if isTransientGRPCError(err) {
				r.log.Warn().
					Err(err).
					Int64("bytes_read", r.bytesRead).
					Msg("transient gRPC error receiving, retrying from current position")
				r.stream = nil
				continue
			}

			return 0, err
		}

		// Check for unexpected header
		if msg.GetHeader() != nil {
			return 0, fmt.Errorf("received unexpected header after first message")
		}

		data := msg.GetData()
		if data == nil {
			return 0, fmt.Errorf("received message with neither header nor data")
		}

		// Check for excess data
		if r.bytesRead+int64(len(data.Data)) > r.totalLength {
			return 0, fmt.Errorf("remote node sent more data than requested (%d received, %d expected)", r.bytesRead+int64(len(data.Data)), r.totalLength)
		}

		// Buffer the data
		r.buffer = data.Data
		r.bufferPos = 0
		// Loop back to serve from buffer
	}
}

func (r *retryingStreamReader) Close() error {
	r.closed = true
	return nil
}

// isRetryableError checks if a remote error code should trigger a retry.
//
// Retryable error codes:
// - ERROR_CODE_WRONG_NODE: Resolution race, node migration, stale connection
//
// Non-retryable error codes (business logic errors):
// - ERROR_CODE_NOT_FOUND: Chunk doesn't exist
// - ERROR_CODE_VERSION_MISMATCH: Concurrent modification
// - ERROR_CODE_WRITER_KEY_MISMATCH: Wrong writer key
// - ERROR_CODE_WRONG_STATE: Chunk in wrong state
// - ERROR_CODE_INVALID_ARGUMENT: Malformed request
func isRetryableError(code chunkv1.ErrorCode) bool {
	switch code {
	case chunkv1.ErrorCode_ERROR_CODE_WRONG_NODE:
		return true
	default:
		return false
	}
}

// isTransientGRPCError checks if a gRPC error is transient and should trigger a retry.
//
// Transient gRPC codes:
// - Unavailable: Service temporarily unavailable (network issue, node restart)
// - DeadlineExceeded: Request took too long (might succeed on retry)
// - ResourceExhausted: Rate limiting, backpressure (might succeed after backoff)
//
// Non-transient gRPC codes:
// - InvalidArgument: Malformed request (won't succeed on retry)
// - NotFound: Resource doesn't exist (won't succeed on retry)
// - PermissionDenied: Authentication/authorization failure (won't succeed on retry)
func isTransientGRPCError(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}

	switch st.Code() {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted:
		return true
	}

	return false
}
