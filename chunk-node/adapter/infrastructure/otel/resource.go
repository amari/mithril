package otel

import (
	"context"
	"os"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

func NewResource() (*resource.Resource, error) {
	var attrs []attribute.KeyValue

	// Standard service fields
	if serviceName := os.Getenv("OTEL_SERVICE_NAME"); serviceName != "" {
		attrs = append(attrs, semconv.ServiceName(serviceName))
	}
	if serviceVersion := os.Getenv("OTEL_SERVICE_VERSION"); serviceVersion != "" {
		attrs = append(attrs, semconv.ServiceVersion(serviceVersion))
	}
	if serviceNamespace := os.Getenv("OTEL_SERVICE_NAMESPACE"); serviceNamespace != "" {
		attrs = append(attrs, semconv.ServiceNamespace(serviceNamespace))
	}

	// parse OTEL_RESOURCE_ATTRIBUTES
	if rawAttrs := os.Getenv("OTEL_RESOURCE_ATTRIBUTES"); rawAttrs != "" {
		for pair := range strings.SplitSeq(rawAttrs, ",") {
			kv := strings.SplitN(pair, "=", 2)
			if len(kv) == 2 {
				key := strings.TrimSpace(kv[0])
				value := strings.TrimSpace(kv[1])
				attrs = append(attrs, attribute.String(key, value))
			}
		}
	}

	return resource.New(context.Background(),
		resource.WithAttributes(attrs...),
		resource.WithFromEnv(),      // Also allow OTEL_RESOURCE_ATTRIBUTES
		resource.WithTelemetrySDK(), // Add SDK information
		resource.WithHost(),         // Add host information
	)
}
