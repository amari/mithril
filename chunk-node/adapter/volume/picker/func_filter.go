package picker

import (
	"github.com/amari/mithril/chunk-node/domain"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
)

type PickFilterFunc func(v domain.VolumeID) bool

var _ portvolume.VolumeIDPickFilter = PickFilterFunc(nil)

func (f PickFilterFunc) FilterVolumeIDPick(v domain.VolumeID) bool {
	return f(v)
}
