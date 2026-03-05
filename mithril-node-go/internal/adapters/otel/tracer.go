package adaptersotel

import "go.opentelemetry.io/otel/trace"

func NewTracer(tp trace.TracerProvider) trace.Tracer {
	return tp.Tracer("mithril-node-go")
}
