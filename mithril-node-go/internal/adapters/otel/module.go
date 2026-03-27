package adaptersotel

import (
	applicationservices "github.com/amari/mithril/mithril-node-go/internal/application/services"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/fx"
)

func Module() fx.Option {
	opts := []fx.Option{
		fx.Provide(
			NewMeter,
			NewMeterProvider,
			NewResource,
			NewTextMapPropagator,
			NewTracer,
			NewTracerProvider,
			fx.Annotate(NewVolumeMetricsExporter, fx.As(new(applicationservices.VolumeMetricsExporter))),
		),
		fx.Invoke(
			otel.SetMeterProvider,
			otel.SetTextMapPropagator,
			otel.SetTracerProvider,
			func(mp metric.MeterProvider, lc fx.Lifecycle) {
				lc.Append(fx.StartHook(func() error {
					return runtime.Start(runtime.WithMeterProvider(mp))
				}))
			},
			func(e applicationservices.VolumeMetricsExporter, lc fx.Lifecycle) {
				starter, ok := e.(interface{ start() error })
				if ok {
					lc.Append(fx.StartHook(starter.start))
				}

				stopper, ok := e.(interface{ stop() error })
				if ok {
					lc.Append(fx.StopHook(stopper.stop))
				}
			},
		),
	}

	return fx.Options(opts...)
}
