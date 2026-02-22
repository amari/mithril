package directory

import (
	"github.com/amari/mithril/chunk-node/domain"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
)

type CharacteristicsProvider struct {
	characteristics *domain.VolumeCharacteristics
}

var _ portvolume.VolumeCharacteristicsProvider = (*CharacteristicsProvider)(nil)

func NewCharacteristicsProvider(path string) (*CharacteristicsProvider, error) {
	characteristics, err := getVolumeCharacteristicsForPath(path)
	if err != nil {
		return nil, err
	}

	return &CharacteristicsProvider{
		characteristics: characteristics,
	}, nil
}

func (p *CharacteristicsProvider) GetVolumeCharacteristics() *domain.VolumeCharacteristics {
	return p.characteristics
}
