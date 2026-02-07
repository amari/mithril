package volume

import (
	"sync"

	"github.com/amari/mithril/chunk-node/domain"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
	"go.opentelemetry.io/otel/attribute"
)

type VolumeAttributeRegistry struct {
	mu    sync.RWMutex
	attrs map[domain.VolumeID]map[string]attribute.KeyValue
}

var _ portvolume.VolumeAttributeRegistry = (*VolumeAttributeRegistry)(nil)

func NewVolumeAttributeRegistry() portvolume.VolumeAttributeRegistry {
	return &VolumeAttributeRegistry{
		attrs: make(map[domain.VolumeID]map[string]attribute.KeyValue),
	}
}

func (r *VolumeAttributeRegistry) SetAttributes(volumeID domain.VolumeID, attrs []attribute.KeyValue) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(attrs) == 0 {
		delete(r.attrs, volumeID)
		return
	}

	attrMap := make(map[string]attribute.KeyValue, len(attrs))
	for _, attr := range attrs {
		attrMap[string(attr.Key)] = attr
	}
	r.attrs[volumeID] = attrMap
}

func (r *VolumeAttributeRegistry) AddAttributes(volumeID domain.VolumeID, attrs ...attribute.KeyValue) {
	if len(attrs) == 0 {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	attrMap, exists := r.attrs[volumeID]
	if !exists {
		attrMap = make(map[string]attribute.KeyValue)
		r.attrs[volumeID] = attrMap
	}

	for _, attr := range attrs {
		attrMap[string(attr.Key)] = attr
	}
}

func (r *VolumeAttributeRegistry) RemoveAttributes(volumeID domain.VolumeID, keys ...string) {
	if len(keys) == 0 {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	attrMap, exists := r.attrs[volumeID]
	if !exists {
		return
	}

	for _, key := range keys {
		delete(attrMap, key)
	}

	// Clean up empty map
	if len(attrMap) == 0 {
		delete(r.attrs, volumeID)
	}
}

func (r *VolumeAttributeRegistry) RemoveAllAttributes(volumeID domain.VolumeID) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.attrs, volumeID)
}

func (r *VolumeAttributeRegistry) GetAttributes(volumeID domain.VolumeID) []attribute.KeyValue {
	r.mu.RLock()
	defer r.mu.RUnlock()

	attrMap, exists := r.attrs[volumeID]
	if !exists || len(attrMap) == 0 {
		return nil
	}

	// Convert map to slice
	attrs := make([]attribute.KeyValue, 0, len(attrMap))
	for _, attr := range attrMap {
		attrs = append(attrs, attr)
	}

	return attrs
}
