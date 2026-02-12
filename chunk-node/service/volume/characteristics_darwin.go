//go:build darwin
// +build darwin

package volume

import (
	"github.com/amari/mithril/chunk-node/adapter/volume/characteristics/darwin"
	"github.com/amari/mithril/chunk-node/domain"
)

// GetVolumeCharacteristicsForPath returns the volume characteristics for the given filesystem path.
func GetVolumeCharacteristicsForPath(path string) (*domain.VolumeCharacteristics, error) {
	return darwin.GetVolumeCharacteristicsForPath(path)
}
