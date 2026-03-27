package adaptersotel

import (
	"os"
	"strings"

	"go.opentelemetry.io/otel/propagation"
)

func NewTextMapPropagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
}

func otelTracePropagatorFromEnv() propagation.TextMapPropagator {
	env := os.Getenv("OTEL_PROPAGATORS")
	if env == "" {
		env = "tracecontext,baggage" // default according to OpenTelemetry spec
	}

	propagators := []propagation.TextMapPropagator{}

	// TODO: add support for b3, b3multi, xray, etc.
	for _, p := range strings.Split(env, ",") {
		switch strings.TrimSpace(strings.ToLower(p)) {
		case "tracecontext":
			propagators = append(propagators, propagation.TraceContext{})
		case "baggage":
			propagators = append(propagators, propagation.Baggage{})
		}
	}

	if len(propagators) == 0 {
		return propagation.NewCompositeTextMapPropagator()
	}

	return propagation.NewCompositeTextMapPropagator(propagators...)
}
