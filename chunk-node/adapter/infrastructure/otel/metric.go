package otel

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	exportersprometheus "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.uber.org/fx"
)

func NewMeterProvider(r *resource.Resource, log *zerolog.Logger, lc fx.Lifecycle, sd fx.Shutdowner) (metric.MeterProvider, error) {
	exporterType := strings.ToLower(os.Getenv("OTEL_METRICS_EXPORTER"))
	if exporterType == "" {
		exporterType = "none"
	}

	var reader sdkmetric.Reader

	switch exporterType {
	case "none":
		// No exporter
	case "prometheus":
		exporterOpts := []exportersprometheus.Option{
			// exportersprometheus.WithoutUnits(),
			// exportersprometheus.WithoutScopeInfo(),
			// exportersprometheus.WithAggregationSelector(defaultAggregationSelector),
			// exportersprometheus.WithRegisterer(prometheus.DefaultRegisterer),
		}

		exporter, err := exportersprometheus.New(exporterOpts...)
		if err != nil {
			return nil, fmt.Errorf("failed to create Prometheus exporter: %w", err)
		}

		reader = exporter

	default:
		return nil, fmt.Errorf("unsupported OTEL_METRICS_EXPORTER: %q", exporterType)
	}

	metricProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(r),
		sdkmetric.WithReader(reader),
	)

	lc.Append(fx.StopHook(metricProvider.Shutdown))

	if exporterType == "prometheus" {
		host := os.Getenv("OTEL_EXPORTER_PROMETHEUS_HOST")

		port := os.Getenv("OTEL_EXPORTER_PROMETHEUS_PORT")
		if port == "" {
			port = "9464"
		} else if _, err := strconv.ParseUint(port, 10, 16); err != nil {
			return nil, fmt.Errorf("unsupported OTEL_EXPORTER_PROMETHEUS_PORT: %q", port)
		}

		address := net.JoinHostPort(host, port)

		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())

		server := &http.Server{
			Handler:      mux,
			ReadTimeout:  5 * time.Second,
			WriteTimeout: 10 * time.Second,
			IdleTimeout:  120 * time.Second,
		}

		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				listenCfg := net.ListenConfig{}

				lis, err := listenCfg.Listen(ctx, "tcp", address)
				if err != nil {
					return err
				}

				log.Info().
					Str(string(semconv.ServerAddressKey), lis.Addr().String()).
					Msg("prometheus server listening")

				go func() {
					defer lis.Close()

					if err := server.Serve(lis); err != nil {
						if errors.Is(err, http.ErrServerClosed) {
							return
						}

						log.Error().Err(err).Str("address", address).Msg("prometheus server error")

						// Initiate shutdown
						_ = sd.Shutdown()
					}
				}()
				return nil
			},
			OnStop: func(ctx context.Context) error {
				return server.Shutdown(ctx)
			},
		})
	}

	return metricProvider, nil
}
