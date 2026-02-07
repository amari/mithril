package node

import (
	"context"
	"fmt"
	"sync"

	"github.com/amari/mithril/chunk-node/domain"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc"
)

// MemberConnectionManager pools gRPC connections to cluster members.
// Each NodeID gets a single connection that is reused across all operations.
// The chunk-node resolver embedded in each connection handles lifecycle,
// health checking, and automatic re-resolution on node restarts.
type MemberConnectionManager struct {
	dialOptions []grpc.DialOption
	log         *zerolog.Logger

	// Metrics
	poolHits   metric.Int64Counter
	poolMisses metric.Int64Counter

	mu    sync.RWMutex
	conns map[domain.NodeID]*grpc.ClientConn
}

// NewMemberConnectionManager creates a connection manager.
// All connections share the provided dial options (TLS, auth, timeouts, etc.).
func NewMemberConnectionManager(
	dialOptions []grpc.DialOption,
	meter metric.Meter,
	log *zerolog.Logger,
) (*MemberConnectionManager, error) {
	poolHits, err := meter.Int64Counter("chunk_node.connection_pool.hits")
	if err != nil {
		return nil, fmt.Errorf("failed to create pool hits counter: %w", err)
	}

	poolMisses, err := meter.Int64Counter("chunk_node.connection_pool.misses")
	if err != nil {
		return nil, fmt.Errorf("failed to create pool misses counter: %w", err)
	}

	return &MemberConnectionManager{
		dialOptions: dialOptions,
		log:         log,
		poolHits:    poolHits,
		poolMisses:  poolMisses,
		conns:       make(map[domain.NodeID]*grpc.ClientConn),
	}, nil
}

// GetConnection returns a gRPC connection to the specified node.
// Connections are pooled and reused. The embedded chunk-node resolver
// handles all lifecycle, health checking, and restart detection automatically.
func (m *MemberConnectionManager) GetConnection(ctx context.Context, nodeID domain.NodeID) (*grpc.ClientConn, error) {
	// Check pool (fast path)
	m.mu.RLock()
	conn, exists := m.conns[nodeID]
	m.mu.RUnlock()

	if exists {
		m.poolHits.Add(ctx, 1)
		return conn, nil
	}

	// Pool miss - need to dial
	m.poolMisses.Add(ctx, 1)

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock
	if conn, exists := m.conns[nodeID]; exists {
		return conn, nil
	}

	// Dial new connection using chunk-node resolver
	target := fmt.Sprintf("chunk-node:///%08x", nodeID)

	m.log.Debug().
		Uint32("node_id", uint32(nodeID)).
		Str("target", target).
		Msg("dialing new connection to cluster member")

	conn, err := grpc.NewClient(target, m.dialOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to create client for node %08x: %w", nodeID, err)
	}

	// Store in pool
	m.conns[nodeID] = conn

	m.log.Debug().
		Uint32("node_id", uint32(nodeID)).
		Msg("established new gRPC connection to cluster member")

	return conn, nil
}

// Close closes all pooled connections.
func (m *MemberConnectionManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var firstErr error

	for nodeID, conn := range m.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(m.conns, nodeID)

		m.log.Debug().
			Uint32("node_id", uint32(nodeID)).
			Msg("closed connection to cluster member")
	}

	m.log.Info().Msg("closed all member connections")

	return firstErr
}
