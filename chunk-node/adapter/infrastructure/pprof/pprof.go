package pprof

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/rs/zerolog"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.uber.org/fx"
)

type Config struct {
	Enabled bool   `koanf:"enabled"`
	Address string `koanf:"address"`
}

func Module(cfg *Config) fx.Option {
	return fx.Module("infrastructure.pprof",
		fx.Invoke(func(log *zerolog.Logger, lc fx.Lifecycle, sd fx.Shutdowner) error {
			if !cfg.Enabled {
				return nil
			}

			address := cfg.Address
			if address == "" {
				address = "localhost:6060"
			}

			mux := http.NewServeMux()
			mux.HandleFunc("/debug/pprof/", pprof.Index)
			mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
			mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
			mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
			mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
			mux.Handle("/debug/pprof/heap", pprof.Handler("heap"))
			mux.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
			mux.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
			mux.Handle("/debug/pprof/block", pprof.Handler("block"))
			mux.Handle("/debug/pprof/mutex", pprof.Handler("mutex"))
			mux.Handle("/debug/pprof/allocs", pprof.Handler("allocs"))

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
						Msg("pprof server listening")

					go func() {
						defer lis.Close()

						if err := server.Serve(lis); err != nil {
							if errors.Is(err, http.ErrServerClosed) {
								return
							}

							log.Error().Err(err).Str("address", address).Msg("pprof server error")

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
