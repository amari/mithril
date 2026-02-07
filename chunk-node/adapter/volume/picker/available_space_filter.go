package picker

import (
	"context"

	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/port/volume"
)

type AvailableSpacePickFilter struct {
	MinFreeSpaceBytes   int64
	VolumeStatsProvider volume.VolumeStatsProvider
}

var _ volume.VolumeIDPickFilter = (*AvailableSpacePickFilter)(nil)

func (f *AvailableSpacePickFilter) FilterVolumeIDPick(v domain.VolumeID) bool {
	if f.VolumeStatsProvider == nil {
		return true
	}

	stats, err := f.VolumeStatsProvider.VolumeStats(context.Background(), v)
	if err != nil {
		return false
	}

	if stats.SpaceUtilization.Value == nil {
		return false
	}

	// Use FreeBytes directly instead of calculating
	return stats.SpaceUtilization.Value.FreeBytes >= f.MinFreeSpaceBytes
}
