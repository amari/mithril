package adaptersotel

import (
	"go.opentelemetry.io/otel/metric"
)

func NewMeter(mp metric.MeterProvider) metric.Meter {
	return mp.Meter("mithril-node-go")
}
