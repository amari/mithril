package node

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/amari/mithril/chunk-node/domain"
	portnode "github.com/amari/mithril/chunk-node/port/node"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// EtcdMemberResolver resolves node IDs to member info by reading from etcd.
type EtcdMemberResolver struct {
	client *clientv3.Client
}

var _ portnode.MemberResolver = (*EtcdMemberResolver)(nil)

// NewMemberResolver creates an etcd-backed member resolver.
func NewMemberResolver(client *clientv3.Client) portnode.MemberResolver {
	return &EtcdMemberResolver{
		client: client,
	}
}

// ResolveMember looks up a node's connection info from etcd.
func (r *EtcdMemberResolver) ResolveMember(ctx context.Context, nodeID domain.NodeID) (*portnode.Member, error) {
	key := fmt.Sprintf("/mithril/cluster/presence/nodes/%08x", nodeID)

	resp, err := r.client.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get node announcement from etcd: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("node %08x not found in cluster", nodeID)
	}

	var announcement struct {
		StartupNonce string   `json:"startupNonce"`
		GRPCURLs     []string `json:"grpcURLs"`
	}

	if err := json.Unmarshal(resp.Kvs[0].Value, &announcement); err != nil {
		return nil, fmt.Errorf("failed to unmarshal node announcement: %w", err)
	}

	startupNonce := uint64(0)
	if announcement.StartupNonce != "" {
		if _, err := fmt.Sscanf(announcement.StartupNonce, "%d", &startupNonce); err != nil {
			return nil, fmt.Errorf("failed to parse startup nonce: %w", err)
		}
	}

	return &portnode.Member{
		NodeID:       nodeID,
		StartupNonce: startupNonce,
		GRPCURLs:     announcement.GRPCURLs,
	}, nil
}
