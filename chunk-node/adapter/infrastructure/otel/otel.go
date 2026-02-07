package otel

import (
	"go.opentelemetry.io/otel"
	"go.uber.org/fx"
)

func Module() fx.Option {
	return fx.Module("infrastructure.opentelemetry",
		fx.Provide(NewResource),
		fx.Provide(NewMeterProvider),
		fx.Provide(NewTracerProvider),
		fx.Provide(NewMeter),
		fx.Invoke(otel.SetMeterProvider),
		fx.Invoke(otel.SetTracerProvider),
	)
}
