package otel

import "go.opentelemetry.io/otel/metric"

// In adapter/infrastructure/otel/otel.go or metric.go

func NewMeter(mp metric.MeterProvider) metric.Meter {
	return mp.Meter("github.com/amari/mithril/chunk-node")
}
