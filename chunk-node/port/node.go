package port

import (
	"context"

	"github.com/amari/mithril/chunk-node/domain"
)

type NodeSeedGenerator interface {
	GenerateNodeSeed(ctx context.Context) (domain.NodeSeed, error)
}

type NodeSeedRepository interface {
	LoadNodeSeed(ctx context.Context) (domain.NodeSeed, error)
	StoreNodeSeed(ctx context.Context, seed domain.NodeSeed) error
}

type NodeIdentityAllocator interface {
	AllocateNodeIdentity(ctx context.Context, seed domain.NodeSeed) (*domain.NodeIdentity, error)
	ValidateNodeIdentity(ctx context.Context, identity *domain.NodeIdentity) error
}

type NodeIdentityRepository interface {
	LoadNodeIdentity(ctx context.Context) (*domain.NodeIdentity, error)
	StoreNodeIdentity(ctx context.Context, identity *domain.NodeIdentity) error
}

type NodeAnnouncer interface {
	SetAnnouncement(ctx context.Context, announcement *domain.NodeAnnouncement) error
	ClearAnnouncement(ctx context.Context) error
}

type NodeLabelPublisher interface {
	PublishLabels(ctx context.Context, labels map[string]string) error
}

type NodeWeightPublisher interface {
	PublishWeight(ctx context.Context, weight int) error
}

type NodeVolumeLabelPublisher interface {
	PublishVolumeLabels(ctx context.Context, volumeLabels map[string]map[string][]byte) error
}

type NodeLabeler interface {
	Labels(ctx context.Context) (map[string]string, error)
}
