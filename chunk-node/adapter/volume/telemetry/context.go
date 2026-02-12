package adaptervolumetelemetry

import (
	"context"

	"github.com/amari/mithril/chunk-node/domain"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/trace"
)

// WithVolumeTelemetry adds volume-related telemetry information to the context, such as logger fields and trace attributes.
func WithVolumeTelemetry(ctx context.Context, id domain.VolumeID, telemetry portvolume.VolumeTelemetryProvider) context.Context {
	if telemetry == nil {
		return ctx
	}

	logger := zerolog.Ctx(ctx)
	if logger != nil {
		newL := logger.With().Fields(telemetry.GetVolumeLoggerFields(id)).Logger()
		ctx = newL.WithContext(ctx)
	}
	ctx = logger.WithContext(ctx)

	span := trace.SpanFromContext(ctx)
	if span != nil {
		span.SetAttributes(telemetry.GetVolumeAttributes(id)...)
	}

	return ctx
}
