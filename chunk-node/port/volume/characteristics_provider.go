package portvolume

import "github.com/amari/mithril/chunk-node/domain"

// VolumeCharacteristicsProvider defines an interface for retrieving volume characteristics by volume ID.
type VolumeCharacteristicsProvider interface {
	// GetVolumeCharacteristics returns the characteristics of the volume with the given ID.
	GetVolumeCharacteristics(id domain.VolumeID) (*domain.VolumeCharacteristics, bool)
}
