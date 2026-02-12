package volume

import (
	"context"

	"github.com/amari/mithril/chunk-node/domain"
)

type Stats = domain.VolumeStats

// VolumeStatsProvider provides statistics for a given volume.
type VolumeStatsProvider interface {
	VolumeStats(ctx context.Context, volume domain.VolumeID) (domain.VolumeStats, error)

	// TODO: refactor to `GetVolumeStats(id ID) (Stats, error)`
}
