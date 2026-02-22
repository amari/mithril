package portvolume

import (
	"github.com/amari/mithril/chunk-node/domain"
)

// VolumeCharacteristicsProvider defines an interface for retrieving volume characteristics.
type VolumeCharacteristicsProvider interface {
	GetVolumeCharacteristics() *domain.VolumeCharacteristics
}

type VolumeIDToCharacteristicsIndex interface {
	// GetVolumeCharacteristicsByID returns the volume characteristics associated with the given volume ID.
	GetVolumeCharacteristicsByID(id domain.VolumeID) *domain.VolumeCharacteristics
}
