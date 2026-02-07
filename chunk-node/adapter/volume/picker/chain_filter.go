package picker

import (
	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/port/volume"
)

type chainPickFilter []volume.VolumeIDPickFilter

var _ volume.VolumeIDPickFilter = (*chainPickFilter)(nil)

func ChainPickFilter(filters ...volume.VolumeIDPickFilter) volume.VolumeIDPickFilter {
	return chainPickFilter(filters)
}

func (f chainPickFilter) FilterVolumeIDPick(v domain.VolumeID) bool {
	for _, filter := range f {
		if !filter.FilterVolumeIDPick(v) {
			return false
		}
	}
	return true
}
