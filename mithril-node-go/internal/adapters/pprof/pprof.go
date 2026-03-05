package adapterspprof

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/pprof"
	"time"

	configpprof "github.com/amari/mithril/mithril-node-go/internal/config/pprof"
	"github.com/hellofresh/health-go/v5"
	"github.com/rs/zerolog"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.uber.org/fx"
)

func Module(pprofCfg *configpprof.Server) fx.Option {
	return fx.Options(
		fx.Invoke(func(h *health.Health, log *zerolog.Logger, lc fx.Lifecycle, sd fx.Shutdowner) error {
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

			lc.Append(fx.StartStopHook(
				func(ctx context.Context) error {
					listenCfg := net.ListenConfig{}

					lis, err := listenCfg.Listen(ctx, "tcp", pprofCfg.Listen)
					if err != nil {
						return err
					}

					log.Info().
						Str(string(semconv.ServerAddressKey), lis.Addr().String()).
						Msg("pprof server started")

					go func() {
						defer lis.Close()

						if err := server.Serve(lis); err != nil {
							if errors.Is(err, http.ErrServerClosed) {
								return
							}

							log.
								Error().
								Err(err).
								Str(string(semconv.ServerAddressKey), lis.Addr().String()).
								Msg("pprof server error")

							// Initiate shutdown
							_ = sd.Shutdown()
						}
					}()
					return nil
				},
				server.Shutdown,
			))

			return nil
		}),
	)
}
