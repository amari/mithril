package healthcheck

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/hellofresh/health-go/v5"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.uber.org/fx"
)

type Config struct {
	Address string `koanf:"address"`
}

func Module(cfg *Config) fx.Option {
	return fx.Module("infrastructure.healthcheck",
		fx.Provide(fx.Annotate(func(otelResource *resource.Resource) (*health.Health, error) {
			var healthComponent health.Component

			if otelResource != nil {
				for _, attr := range otelResource.Attributes() {
					switch attr.Key {
					case semconv.ServiceNameKey:
						healthComponent.Name = attr.Value.AsString()
					case semconv.ServiceVersionKey:
						healthComponent.Version = attr.Value.AsString()
					}
				}
			}

			return health.New(health.WithComponent(healthComponent))
		}, fx.ParamTags(`optional:"true"`))),

		fx.Invoke(func(h *health.Health, log *zerolog.Logger, lc fx.Lifecycle, sd fx.Shutdowner) error {
			address := cfg.Address
			if address == "" {
				address = "localhost:8081"
			}

			mux := http.NewServeMux()

			// Liveness: simple OK response
			mux.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("ok"))
			})

			// Readiness: use health-go
			mux.Handle("/readyz", h.Handler())

			// Alias /healthz to /readyz
			mux.Handle("/healthz", h.Handler())

			server := &http.Server{
				Handler: mux,
				// Reasonable production defaults
				ReadTimeout:       15 * time.Second,
				ReadHeaderTimeout: 5 * time.Second,
				WriteTimeout:      30 * time.Second,
				IdleTimeout:       120 * time.Second,
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
						Msg("healthcheck server listening")

					go func() {
						defer lis.Close()

						if err := server.Serve(lis); err != nil {
							if errors.Is(err, http.ErrServerClosed) {
								return
							}

							log.Error().Err(err).Str("address", address).Msg("healthcheck server error")

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
			return nil
		}),
	)
}
