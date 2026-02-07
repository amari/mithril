package node

import (
	"context"

	"github.com/amari/mithril/chunk-node/domain"
)

// ClusterMemberEvent represents a change in cluster membership
type ClusterMemberEvent struct {
	NodeID domain.NodeID
	Type   ClusterMemberEventType
	Node   *Member // nil for ClusterMemberEventTypeLeft events
}

type ClusterMemberEventType int

const (
	ClusterMemberEventTypeJoined  ClusterMemberEventType = iota // Node announced itself
	ClusterMemberEventTypeUpdated                               // Announcement changed (e.g., new StartupNonce)
	ClusterMemberEventTypeLeft                                  // Node announcement expired/deleted
)

// ClusterMemberObserver observes changes in cluster node membership
type ClusterMemberObserver interface {
	// Observe returns a channel that receives cluster membership events.
	// The channel is closed when the context is canceled.
	// Implementations may use etcd watch API or polling.
	Observe(ctx context.Context) (<-chan ClusterMemberEvent, error)
}
