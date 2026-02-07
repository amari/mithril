package picker

import (
	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/port/volume"
)

type PickFilterFunc func(v domain.VolumeID) bool

var _ volume.VolumeIDPickFilter = PickFilterFunc(nil)

func (f PickFilterFunc) FilterVolumeIDPick(v domain.VolumeID) bool {
	return f(v)
}
