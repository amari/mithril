package portnode

import (
	"context"

	"github.com/amari/mithril/chunk-node/domain"
)

// MemberWatchManager provides per-node event subscriptions with automatic lifecycle management.
type MemberWatchManager interface {
	// Watch returns a channel that receives events for the specified node.
	// The subscription remains active until the context is canceled.
	// The returned channel is closed when the context is canceled.
	//
	// The underlying watch is shared across multiple subscribers to the same node.
	// When the last subscriber's context is canceled, the watch stops after a grace period.
	// If a new subscriber arrives during the grace period, the watch is reused.
	Watch(ctx context.Context, nodeID domain.NodeID) (<-chan ClusterMemberEvent, error)
}
