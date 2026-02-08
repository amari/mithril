package picker

import (
	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/port/volume"
)

type HealthPickFilter struct {
	HealthChecker volume.VolumeHealthChecker
}

var _ volume.VolumeIDPickFilter = (*HealthPickFilter)(nil)

func (f *HealthPickFilter) FilterVolumeIDPick(v domain.VolumeID) bool {
	if f.HealthChecker == nil {
		return true
	}

	h := f.HealthChecker.CheckVolumeHealth(v)

	return h.State == domain.VolumeStateOK
}
