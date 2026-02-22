package portvolume

import (
	"github.com/amari/mithril/chunk-node/domain"
)

// VolumeStatsProvider provides statistics for a given volume.
type VolumeStatsProvider interface {
	GetVolumeStats() *domain.VolumeStats
}

type VolumeIDToStatsIndex interface {
	GetVolumeStatsByID(id domain.VolumeID) *domain.VolumeStats
}
