package volume

import (
	"github.com/amari/mithril/chunk-node/domain"
)

// VolumeStatsProvider provides statistics for a given volume.
type VolumeStatsProvider interface {
	GetVolumeStats(id ID) *domain.VolumeStats
}
