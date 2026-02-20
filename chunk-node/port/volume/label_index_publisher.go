package portvolume

import (
	"github.com/amari/mithril/chunk-node/domain"
)

type VolumeIDSetLabelIndexesPublisher interface {
	PublishVolumeIDSetLabelIndexes(volumeIDSetLabelIndexes map[string]map[string]*domain.VolumeIDSet) error
}
