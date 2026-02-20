package portvolume

import (
	"context"

	"github.com/amari/mithril/chunk-node/domain"
)

type VolumeIDAllocator interface {
	AllocateVolumeID(ctx context.Context) (domain.VolumeID, error)
}
