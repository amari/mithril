package node

import (
	"context"

	"github.com/amari/mithril/chunk-node/domain"
)

// Member represents a point-in-time resolution of a cluster member's connection information.
// This information can become stale quickly (e.g., node restarts, pod migrations, IP changes).
type Member struct {
	NodeID       domain.NodeID
	StartupNonce uint64 // Detects node restarts
	GRPCURLs     []string
}

// MemberResolver resolves node IDs to their announced connection information.
// Implementations should make resolution cheap (e.g., via caching, watching)
// so callers can resolve on every use without performance concerns.
type MemberResolver interface {
	// ResolveMember looks up a node by its ID and returns its current connection information.
	// Returns an error if the node is not found or not currently announced.
	// Callers should resolve on every use rather than caching Member values.
	ResolveMember(ctx context.Context, nodeID domain.NodeID) (*Member, error)
}
