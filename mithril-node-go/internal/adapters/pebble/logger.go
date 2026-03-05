package adapterspebble

import (
	"context"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type LoggerAndTracer struct {
	Logger     *zerolog.Logger
	Tracer     trace.Tracer
	Attributes []attribute.KeyValue
}

var _ pebble.LoggerAndTracer = (*LoggerAndTracer)(nil)

func (l *LoggerAndTracer) Infof(format string, args ...any) {
	l.Logger.Info().Msgf(format, args...)
}

func (l *LoggerAndTracer) Errorf(format string, args ...any) {
	l.Logger.Error().Msgf(format, args...)
}

func (l *LoggerAndTracer) Fatalf(format string, args ...any) {
	l.Logger.Fatal().Msgf(format, args...)
}

func (l *LoggerAndTracer) Eventf(ctx context.Context, format string, args ...any) {
	span := trace.SpanFromContext(ctx)

	if span.IsRecording() {
		_, span = l.Tracer.Start(ctx, "Event")

		span.AddEvent(fmt.Sprintf(format, args...), trace.WithAttributes(l.Attributes...))
	}

	l.Logger.Trace().Msgf(format, args...)
}

func (l *LoggerAndTracer) IsTracingEnabled(ctx context.Context) bool {
	return trace.SpanFromContext(ctx).IsRecording() || l.Logger.GetLevel() == zerolog.TraceLevel
}
