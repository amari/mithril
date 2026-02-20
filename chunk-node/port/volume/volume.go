package portvolume

import (
	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/port"
)

type Volume interface {
	Close() error

	ID() domain.VolumeID

	Chunks() port.ChunkStore
}

// VolumeProvider provides access to volumes by their ID.
type VolumeProvider interface {
	// GetVolume retrieves a volume by its ID.
	// Returns the Volume if found, or nil if the volume does not exist.
	GetVolume(id domain.VolumeID) Volume
}
