package volume

import (
	"context"

	"github.com/amari/mithril/chunk-node/domain"
)

// VolumeStatsProvider provides statistics for a given volume.
type VolumeStatsProvider interface {
	VolumeStats(ctx context.Context, volume domain.VolumeID) (domain.VolumeStats, error)
}
