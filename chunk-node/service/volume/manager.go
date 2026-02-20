package volume

import (
	"slices"
	"sync"

	"github.com/amari/mithril/chunk-node/domain"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
	"github.com/amari/mithril/chunk-node/volumeerrors"
)

type VolumeManager struct {
	mu sync.RWMutex

	volumeSlice []portvolume.Volume
	volumeMap   map[domain.VolumeID]portvolume.Volume
}

func NewVolumeManager() *VolumeManager {
	return &VolumeManager{
		volumeMap: make(map[domain.VolumeID]portvolume.Volume),
	}
}

func (m *VolumeManager) Volumes() []portvolume.Volume {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return slices.Clone(m.volumeSlice)
}

func (m *VolumeManager) VolumeIDs() []domain.VolumeID {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ids := make([]domain.VolumeID, 0, len(m.volumeSlice))
	for _, v := range m.volumeSlice {
		ids = append(ids, v.ID())
	}
	return ids
}

func (m *VolumeManager) GetVolumeByID(id domain.VolumeID) (portvolume.Volume, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	v, ok := m.volumeMap[id]

	if !ok {
		return nil, volumeerrors.ErrNotFound
	}

	return v, nil
}

func (m *VolumeManager) AddVolume(v portvolume.Volume) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.volumeSlice = append(m.volumeSlice, v)
	m.volumeMap[v.ID()] = v
}

func (m *VolumeManager) RemoveVolume(id domain.VolumeID) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.volumeSlice = slices.DeleteFunc(m.volumeSlice, func(v portvolume.Volume) bool {
		return v.ID() == id
	})

	delete(m.volumeMap, id)
}

func (m *VolumeManager) ClearVolumes() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.volumeSlice = nil
	m.volumeMap = make(map[domain.VolumeID]portvolume.Volume)
}

// GetVolume implements the VolumeProvider interface.
func (m *VolumeManager) GetVolume(id domain.VolumeID) portvolume.Volume {
	m.mu.RLock()
	defer m.mu.RUnlock()

	v, ok := m.volumeMap[id]
	if !ok {
		return nil
	}

	return v
}
