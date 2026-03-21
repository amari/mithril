package domain

import (
	"bytes"
	"context"
	"slices"
)

type NodeID uint32

type NodeIDProvider interface {
	GetNodeID() NodeID
}

type NodeSeed []byte

type NodeSeedGenerator interface {
	Generate() (NodeSeed, error)
}

type NodeSeedRepository interface {
	Get(ctx context.Context) (NodeSeed, error)
	Upsert(ctx context.Context, seed NodeSeed) error
}

type NodeClaim struct {
	ID    NodeID
	Proof [32]byte
}

type NodeClaimRepository interface {
	Get(ctx context.Context) (*NodeClaim, error)
	Upsert(ctx context.Context, claim *NodeClaim) error
}

type NodeClaimRegistry interface {
	Register(ctx context.Context, claim *NodeClaim) error
	Check(ctx context.Context, claim *NodeClaim) error
}

type NodeLabelSource interface {
	Read() map[string]string
	Watch(watchCtx context.Context) <-chan struct{}
}

type NodeLabelPublisher interface {
	Publish(node NodeID, labels map[string]string)
}

type NodePresence struct {
	ID    NodeID
	Nonce [32]byte
	GRPC  NodePresenceGRPC
}

func (p *NodePresence) Equals(other *NodePresence) bool {
	return p.ID == other.ID && bytes.Equal(p.Nonce[:], other.Nonce[:]) && slices.Equal(p.GRPC.URLs, other.GRPC.URLs)
}

type NodePresenceGRPC struct {
	URLs []string
}

type NodePresencePublisher interface {
	Publish(presence *NodePresence)
}

type NodePeer struct {
	ID   NodeID
	GRPC NodePeerGRPC
}

type NodePeerGRPC struct {
	URLs []string
}

type NodePeerResolver interface {
	Resolve(ctx context.Context, nodeID NodeID) (*NodePeer, error)
	Watch(ctx context.Context, nodeID NodeID) (<-chan struct{}, error)
}
