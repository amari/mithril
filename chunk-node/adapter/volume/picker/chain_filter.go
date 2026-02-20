package picker

import (
	"github.com/amari/mithril/chunk-node/domain"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
)

type chainPickFilter []portvolume.VolumeIDPickFilter

var _ portvolume.VolumeIDPickFilter = (*chainPickFilter)(nil)

func ChainPickFilter(filters ...portvolume.VolumeIDPickFilter) portvolume.VolumeIDPickFilter {
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
