package picker

import (
	"github.com/amari/mithril/chunk-node/domain"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
)

type AvailableSpacePickFilter struct {
	MinFreeSpaceBytes   int64
	VolumeStatsProvider portvolume.VolumeStatsProvider
}

var _ portvolume.VolumeIDPickFilter = (*AvailableSpacePickFilter)(nil)

func (f *AvailableSpacePickFilter) FilterVolumeIDPick(v domain.VolumeID) bool {
	if f.VolumeStatsProvider == nil {
		return true
	}

	stats := f.VolumeStatsProvider.GetVolumeStats(v)
	if stats == nil {
		return false
	}

	if stats.SpaceUtilization.Value == nil {
		return false
	}

	// Use FreeBytes directly instead of calculating
	return stats.SpaceUtilization.Value.FreeBytes >= f.MinFreeSpaceBytes
}
