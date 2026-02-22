//go:build linux
// +build linux

package directory

import (
	"github.com/amari/mithril/chunk-node/adapter/volume/characteristics/linux"
	"github.com/amari/mithril/chunk-node/domain"
)

func getVolumeCharacteristicsForPath(path string) (*domain.VolumeCharacteristics, error) {
	return linux.GetVolumeCharacteristicsForPath(path)
}
