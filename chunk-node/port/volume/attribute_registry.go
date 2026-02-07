package volume

import (
	"github.com/amari/mithril/chunk-node/domain"
	"go.opentelemetry.io/otel/attribute"
)

// VolumeAttributeRegistry maintains static attributes for volumes.
// Components add attributes when they create/discover volumes and remove them on cleanup.
// These attributes are used to enrich observability (metrics, traces, logs).
type VolumeAttributeRegistry interface {
	// SetAttributes replaces all attributes for a volume.
	// Useful when a component owns all attributes for a volume.
	SetAttributes(volumeID domain.VolumeID, attrs []attribute.KeyValue)

	// AddAttributes adds or updates attributes for a volume (set semantics).
	// If an attribute with the same key exists, it's replaced.
	// This allows multiple components to contribute attributes independently.
	AddAttributes(volumeID domain.VolumeID, attrs ...attribute.KeyValue)

	// RemoveAttributes removes specific attributes by key.
	RemoveAttributes(volumeID domain.VolumeID, keys ...string)

	// RemoveAllAttributes removes all attributes for a volume.
	// Called during volume cleanup.
	RemoveAllAttributes(volumeID domain.VolumeID)

	// GetAttributes returns all current attributes for a volume.
	// Returns nil if volume has no attributes.
	GetAttributes(volumeID domain.VolumeID) []attribute.KeyValue
}
