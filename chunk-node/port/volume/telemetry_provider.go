package portvolume

import (
	"github.com/amari/mithril/chunk-node/domain"
	"go.opentelemetry.io/otel/attribute"
)

// VolumeTelemetryProvider provides observability attributes for volumes.
// Methods return nil if the volume ID is not found.
type VolumeTelemetryProvider interface {
	// GetVolumeAttributes returns OpenTelemetry attributes for tracing and metrics.
	GetVolumeAttributes(id domain.VolumeID) []attribute.KeyValue

	// GetVolumeLoggerFields returns key-value pairs for structured logging.
	GetVolumeLoggerFields(id domain.VolumeID) []any
}
