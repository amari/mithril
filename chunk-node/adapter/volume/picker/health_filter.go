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

	h, err := f.HealthChecker.CheckVolumeHealth(v)
	if err != nil {
		return false
	}

	return h.State == domain.VolumeStateOK
}
