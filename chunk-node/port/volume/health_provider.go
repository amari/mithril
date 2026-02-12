package volume

import "github.com/amari/mithril/chunk-node/domain"

type Health = domain.VolumeHealth

// VolumeHealthProvider provides access to volume health information.
type VolumeHealthProvider interface {
	// GetVolumeHealth returns the health status for the specified volume.
	// Returns nil if the volume does not exist.
	GetVolumeHealth(id ID) *Health
}
