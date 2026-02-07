package grpc

import (
	"context"
	"errors"
	"net"
	"strings"
	"time"

	infrastructuretls "github.com/amari/mithril/chunk-node/adapter/infrastructure/tls"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	semconv "go.opentelemetry.io/otel/semconv/v1.38.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

type ServerConfig struct {
	Address string                          `koanf:"address"`
	TLS     *infrastructuretls.ServerConfig `koanf:"tls"`
}

// ServerModule provides a gRPC server
func ServerModule(cfg *ServerConfig) fx.Option {
	return fx.Module("infrastructure.grpc.server",
		fx.Provide(health.NewServer),

		fx.Provide(func(healthServer *health.Server, log *zerolog.Logger) (*grpc.Server, error) {
			newL := log.With().
				Str(string(semconv.RPCSystemKey), semconv.RPCSystemGRPC.Value.AsString()).
				Logger()

			opts := []grpc.ServerOption{
				grpc.ChainUnaryInterceptor(unaryServerInterceptorZerolog(&newL)),
				grpc.ChainStreamInterceptor(streamServerInterceptorZerolog(&newL)),
				grpc.StatsHandler(otelgrpc.NewServerHandler()),
			}

			if cfg.TLS != nil {
				tlsConfig, err := infrastructuretls.ServerConfigToTLSConfig(cfg.TLS)
				if err != nil {
					return nil, err
				}
				opts = append(opts, grpc.Creds(credentials.NewTLS(tlsConfig)))
			}

			s := grpc.NewServer(opts...)

			healthpb.RegisterHealthServer(s, healthServer)

			reflection.Register(s)

			return s, nil
		}),

		fx.Invoke(func(server *grpc.Server, log *zerolog.Logger, lc fx.Lifecycle, sd fx.Shutdowner) error {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					listenCfg := net.ListenConfig{}

					lis, err := listenCfg.Listen(ctx, "tcp", cfg.Address)
					if err != nil {
						return err
					}

					log.Info().
						Str(string(semconv.ServerAddressKey), lis.Addr().String()).
						Msg("grpc server listening")

					go func() {
						defer lis.Close()

						if err := server.Serve(lis); err != nil {
							if errors.Is(err, grpc.ErrServerStopped) {
								return
							}

							log.Error().Err(err).Str("address", cfg.Address).Msg("gRPC server error")

							// Initiate shutdown
							_ = sd.Shutdown()
						}
					}()
					return nil
				},
				OnStop: func(ctx context.Context) error {
					doneCh := make(chan struct{})
					go func() {
						server.GracefulStop()
						close(doneCh)
					}()

					shutdownCtx, cancelF := context.WithTimeout(ctx, 30*time.Second)
					defer cancelF()

					select {
					case <-shutdownCtx.Done():
						log.Warn().Msg("gRPC server graceful stop timed out, forcing stop")
						server.Stop()
					case <-doneCh:
						// Graceful stop completed
					}

					return nil
				},
			})
			return nil
		}),

		fx.Invoke(func(healthServer *health.Server, lc fx.Lifecycle) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
					return nil
				},
				OnStop: func(ctx context.Context) error {
					healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
					return nil
				},
			})
		}),
	)
}

func zerologContextWithFullMethod(c zerolog.Context, fullMethod string) zerolog.Context {
	if !strings.HasPrefix(fullMethod, "/") {
		return c
	}

	fullMethod = fullMethod[1:]
	i := strings.Index(fullMethod, "/")
	if i < 0 {
		return c
	}

	service := fullMethod[:i]
	method := fullMethod[i+1:]

	return c.
		Str(string(semconv.RPCServiceKey), service).
		Str(string(semconv.RPCMethodKey), method)
}

func zerologContextWithTraceContext(ctx context.Context, fields zerolog.Context) zerolog.Context {
	span := trace.SpanFromContext(ctx)
	sc := span.SpanContext()

	if !sc.IsValid() {
		return fields
	}

	if sc.TraceID().IsValid() {
		fields = fields.Str("trace_id", sc.TraceID().String())
	}
	if sc.SpanID().IsValid() {
		fields = fields.Str("span_id", sc.SpanID().String())
	}
	// TraceFlags is optional; include only if non-zero
	if sc.TraceFlags() != 0 {
		fields = fields.Str("trace_flags", sc.TraceFlags().String())
	}

	return fields
}

func unaryServerInterceptorZerolog(log *zerolog.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp any, err error) {
		newCtx := zerologContextWithTraceContext(ctx, zerologContextWithFullMethod(log.With(), info.FullMethod)).
			Logger().
			WithContext(ctx)

		return handler(newCtx, req)
	}
}

func streamServerInterceptorZerolog(log *zerolog.Logger) grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		newCtx := zerologContextWithTraceContext(ss.Context(), zerologContextWithFullMethod(log.With(), info.FullMethod)).
			Logger().
			WithContext(ss.Context())

		wrapped := ServerStreamWithContext(ss, newCtx)

		return handler(srv, wrapped)
	}
}

func ServerStreamWithContext(ss grpc.ServerStream, ctx context.Context) grpc.ServerStream {
	return &serverStreamWithContext{
		ServerStream: ss,
		ctx:          ctx,
	}
}

type serverStreamWithContext struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *serverStreamWithContext) Context() context.Context {
	return s.ctx
}
