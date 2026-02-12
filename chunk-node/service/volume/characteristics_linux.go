//go:build linux
// +build linux

package volume

import (
	"github.com/amari/mithril/chunk-node/adapter/volume/characteristics/linux"
	"github.com/amari/mithril/chunk-node/domain"
)

// GetVolumeCharacteristicsForPath returns the volume characteristics for the given filesystem path.
func GetVolumeCharacteristicsForPath(path string) (*domain.VolumeCharacteristics, error) {
	return linux.GetVolumeCharacteristicsForPath(path)
}
