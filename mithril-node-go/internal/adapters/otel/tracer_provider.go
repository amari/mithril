package adaptersotel

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/fx"
)

func NewTracerProvider(r *resource.Resource, log *zerolog.Logger, lc fx.Lifecycle, sd fx.Shutdowner) (trace.TracerProvider, error) {
	exporterCtx, cancelF := context.WithCancel(context.Background())
	lc.Append(fx.StopHook(cancelF))

	spanExporter, err := newSpanExporter(exporterCtx)
	if err != nil {
		cancelF()
		return nil, fmt.Errorf("failed to create span exporter: %w", err)
	}

	traceSampler, err := newTraceSampler(log)
	if err != nil {
		cancelF()
		return nil, err
	}

	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(spanExporter),
		sdktrace.WithResource(r),
		sdktrace.WithSampler(traceSampler),
	)

	lc.Append(fx.StopHook(tracerProvider.Shutdown))

	return tracerProvider, nil
}

func newSpanExporter(ctx context.Context) (sdktrace.SpanExporter, error) {
	protocol := os.Getenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL")
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
	if endpoint == "" {
		endpoint = "localhost:4317" // default according to OpenTelemetry spec
	}

	// Build exporter based on protocol

	switch protocol {
	case "grpc", "":
		// Default is gRPC
		return newGRPCSpanExporter(ctx, endpoint)
	case "http/protobuf":
		return newHTTPSpanExporter(ctx, endpoint)
	default:
		return nil, fmt.Errorf("unsupported OTEL_EXPORTER_OTLP_TRACES_PROTOCOL: %s", protocol)
	}
}

func newGRPCSpanExporter(ctx context.Context, endpoint string) (sdktrace.SpanExporter, error) {
	options := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(endpoint),
	}

	insecureStr := os.Getenv("OTEL_EXPORTER_OTLP_TRACES_INSECURE")
	if insecureStr == "" {
		insecureStr = "false"
	}
	insecure, err := strconv.ParseBool(strings.ToLower(insecureStr))
	if err != nil {
		return nil, fmt.Errorf("invalid OTEL_EXPORTER_OTLP_TRACES_INSECURE value: %s", insecureStr)
	}

	if insecure {
		options = append(options, otlptracegrpc.WithInsecure())
	}

	// TODO: add support for TLS configuration via OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE, OTEL_EXPORTER_OTLP_TRACES_CLIENT_CERTIFICATE, OTEL_EXPORTER_OTLP_TRACES_CLIENT_KEY, etc.

	return otlptracegrpc.New(ctx, options...)
}

func newHTTPSpanExporter(ctx context.Context, endpoint string) (sdktrace.SpanExporter, error) {
	// TODO: add support for TLS configuration via OTEL_EXPORTER_OTLP_CERTIFICATE, OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE, OTEL_EXPORTER_OTLP_CLIENT_KEY, etc.

	return otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(endpoint),
	)
}

func newTraceSampler(log *zerolog.Logger) (sdktrace.Sampler, error) {
	name := os.Getenv("OTEL_TRACES_SAMPLER")
	arg := os.Getenv("OTEL_TRACES_SAMPLER_ARG")

	switch strings.ToLower(name) {
	case "always_on":
		return sdktrace.AlwaysSample(), nil
	case "always_off":
		return sdktrace.NeverSample(), nil
	case "traceidratio":
		ratio, err := parseTraceSamplerRatioArg(arg)
		if err != nil {
			return nil, err
		}
		return sdktrace.TraceIDRatioBased(ratio), nil
	case "parentbased_always_on":
		return sdktrace.ParentBased(sdktrace.AlwaysSample()), nil
	case "parentbased_always_off":
		return sdktrace.ParentBased(sdktrace.NeverSample()), nil
	case "parentbased_traceidratio":
		ratio, err := parseTraceSamplerRatioArg(arg)
		if err != nil {
			return nil, err
		}

		return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio)), nil
	case "", "default":
		return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(1.0)), nil
	default:
		// fallback to always_on with warning
		log.Warn().Str("system", "otel").Msgf("unknown OTEL_TRACES_SAMPLER %q, falling back to AlwaysSample", name)

		return sdktrace.AlwaysSample(), nil
	}
}

func parseTraceSamplerRatioArg(arg string) (float64, error) {
	if arg == "" {
		return 1.0, nil
	}

	ratio, err := strconv.ParseFloat(arg, 64)
	if err != nil || ratio < 0 || ratio > 1 {
		return 0, fmt.Errorf("invalid OTEL_TRACES_SAMPLER_ARG: %q", arg)
	}
	return ratio, nil
}
