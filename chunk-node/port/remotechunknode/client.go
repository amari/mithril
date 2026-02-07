package remotechunknode

import (
	"context"
	"io"

	"github.com/amari/mithril/chunk-node/domain"
	chunkv1 "github.com/amari/mithril/gen/go/proto/mithril/chunk/v1"
)

// RemoteChunkClient provides a transport-agnostic interface for reading chunks from remote nodes.
// Implementations handle connection management, retries, topology changes, and error mapping.
//
// Key responsibilities:
// - Resolve NodeID to connection endpoints (via MemberResolver and chunk-node resolver)
// - Handle ERROR_CODE_WRONG_NODE retries (node moved, resolution race, etc.)
// - Map remote ErrorDetails to domain RemoteError types
// - Stream chunk data with robust resume/retry semantics
//
// Architecture:
// This is a port (hexagonal architecture). Adapters implement this interface using
// different transports (gRPC, CSTP). The service layer depends only on this interface,
// not on any specific transport implementation.
type RemoteChunkClient interface {
	// ReadChunkRange reads a range from a remote chunk and returns a streaming reader.
	//
	// The returned io.ReadCloser:
	// - Streams the data progressively (does not buffer entire range in memory)
	// - Validates the stream header (checks total_data_length matches requested length)
	// - Detects and retries on transient errors (uses RetryPolicy from context)
	// - Preserves read offset across retries (resumes from bytes already read)
	// - Returns io.EOF only when all requested bytes have been received
	// - Returns error if remote sends less data (premature EOF) or more data (protocol violation) than requested
	//
	// The ChunkID embeds the target NodeID. If resolution leads to the wrong node,
	// the remote will return ERROR_CODE_WRONG_NODE and the client retries resolution.
	//
	// Retry policy can be configured via context using WithRetryPolicy helper.
	// If no policy is set, sensible defaults are used (3 attempts, exponential backoff).
	//
	// Example:
	//
	//	ctx = remotechunknode.WithRetryPolicy(ctx, remotechunknode.RetryPolicy{
	//	    InitialInterval: 50 * time.Millisecond,
	//	    MaximumAttempts: 5,
	//	})
	//	reader, err := client.ReadChunkRange(ctx, chunkID, offset, length)
	//	if err != nil { ... }
	//	defer reader.Close()
	//	_, err = io.Copy(dest, reader)
	//
	// Error handling:
	// - Returns RemoteError for errors from the remote node (includes remote chunk/volume state)
	// - Returns ChunkError or VolumeError if the remote wraps local errors in RemoteError
	// - Returns transport errors (connection failures, timeouts) as-is
	ReadChunkRange(ctx context.Context, chunkID domain.ChunkID, offset int64, length int64) (io.ReadCloser, error)

	// StatChunk retrieves metadata for a remote chunk.
	// Automatically retries on ERROR_CODE_WRONG_NODE using the retry policy from context.
	// Returns both chunk and volume state from the remote node.
	//
	// Error handling:
	// - Returns RemoteError for errors from the remote node
	// - Returns transport errors as-is
	// - Retries on ERROR_CODE_WRONG_NODE and transient transport errors
	StatChunk(ctx context.Context, chunkID domain.ChunkID) (*chunkv1.Chunk, *chunkv1.Volume, error)
}
