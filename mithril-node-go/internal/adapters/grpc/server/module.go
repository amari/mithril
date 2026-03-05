package adaptersgrpcserver

import (
	"context"
	"errors"
	"net"
	"time"

	chunkv1 "github.com/amari/mithril/gen/go/proto/mithril/chunk/v1"
	configgrpc "github.com/amari/mithril/mithril-node-go/internal/config/grpc"
	configtls "github.com/amari/mithril/mithril-node-go/internal/config/tls"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	grpchealthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

func Module(serverCfg *configgrpc.Server) fx.Option {
	opts := []fx.Option{
		fx.Provide(
			health.NewServer,
			func(logger *zerolog.Logger) (*grpc.Server, error) {
				opts := []grpc.ServerOption{
					grpc.StatsHandler(otelgrpc.NewServerHandler()),
				}

				if serverCfg.TLS != nil {
					tlsConfig, err := configtls.TLSConfigWithServer(serverCfg.TLS)
					if err != nil {
						return nil, err
					}

					opts = append(opts, grpc.Creds(credentials.NewTLS(tlsConfig)))
				} else {
					opts = append(opts, grpc.Creds(insecure.NewCredentials()))
				}

				newLogger := logger.With().
					Str(string(semconv.RPCSystemKey), semconv.RPCSystemGRPC.Value.AsString()).
					Logger()

				opts = append(opts,
					grpc.ChainUnaryInterceptor(zerologUnaryServerInterceptor(&newLogger)),
					grpc.ChainStreamInterceptor(zerologStreamServerInterceptor(&newLogger)),
				)

				s := grpc.NewServer(opts...)

				return s, nil
			},
		),
		fx.Invoke(
			func(s *grpc.Server, lc fx.Lifecycle) {
				lc.Append(fx.StartHook(func() {
					reflection.Register(s)
				}))
			},
			func(svc *health.Server, s *grpc.Server, lc fx.Lifecycle) {
				lc.Append(fx.StartHook(func() {
					grpchealthv1.RegisterHealthServer(s, svc)
				}))
			},
		),
		fx.Provide(
			fx.Annotate(NewChunkServiceServer, fx.As(new(chunkv1.ChunkServiceServer))),
		),
		fx.Invoke(
			func(svc chunkv1.ChunkServiceServer, s *grpc.Server, lc fx.Lifecycle) {
				lc.Append(fx.StartHook(func() {
					chunkv1.RegisterChunkServiceServer(s, svc)
				}))
			},

			func(s *grpc.Server, logger *zerolog.Logger, lc fx.Lifecycle, sd fx.Shutdowner) {
				lc.Append(fx.StartStopHook(
					func(ctx context.Context) error {
						listenCfg := net.ListenConfig{}

						lis, err := listenCfg.Listen(ctx, "tcp", serverCfg.Listen)
						if err != nil {
							return err
						}

						logger.Info().
							Str(string(semconv.ServerAddressKey), lis.Addr().String()).
							Msg("grpc server started")

						go func() {
							defer lis.Close()

							if err := s.Serve(lis); err != nil {
								if errors.Is(err, grpc.ErrServerStopped) {
									return
								}

								logger.Error().Err(err).Str("address", lis.Addr().String()).Msg("gRPC server error")

								// Initiate shutdown
								_ = sd.Shutdown()
							}
						}()

						return nil
					},
					func(ctx context.Context) error {
						doneCh := make(chan struct{})
						go func() {
							s.GracefulStop()

							close(doneCh)
						}()

						shutdownCtx, cancelF := context.WithTimeout(ctx, 5*time.Second)
						defer cancelF()

						select {
						case <-shutdownCtx.Done():
							logger.Warn().Msg("gRPC server graceful stop timed out, forcing stop")

							s.Stop()
						case <-doneCh:
							// Graceful stop completed
						}

						return nil
					},
				))
			},

			func(svc *health.Server, lc fx.Lifecycle) {
				lc.Append(fx.StartStopHook(
					func(ctx context.Context) {
						svc.SetServingStatus("", grpchealthv1.HealthCheckResponse_SERVING)
					},
					func(ctx context.Context) {
						svc.SetServingStatus("", grpchealthv1.HealthCheckResponse_NOT_SERVING)
					},
				))
			},
		),
	}

	return fx.Options(opts...)
}
