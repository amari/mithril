//go:build darwin
// +build darwin

package directory

import (
	"github.com/amari/mithril/chunk-node/adapter/volume/characteristics/darwin"
	"github.com/amari/mithril/chunk-node/domain"
)

func getVolumeCharacteristicsForPath(path string) (*domain.VolumeCharacteristics, error) {
	return darwin.GetVolumeCharacteristicsForPath(path)
}
