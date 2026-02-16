package log

import (
	"fmt"
	"io"
	"log/slog"
	"os"

	"golang.org/x/term"

	fxeventzerolog "github.com/amari/fxevent-zerolog"
	"github.com/rs/zerolog"
	slogzerolog "github.com/samber/slog-zerolog/v2"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
)

func Module(cfg *Config) fx.Option {
	return fx.Options(
		fx.Module("infrastructure.log",
			fx.Provide(func() (*zerolog.Logger, error) {
				lvl := zerolog.InfoLevel
				var w io.Writer = os.Stderr
				var err error

				if cfg.Level != "" {
					lvl, err = cfg.Level.zerologLevel()
					if err != nil {
						return nil, fmt.Errorf("invalid log level %q: %w", cfg.Level, err)
					}
				}

				switch cfg.Format {
				case Console:
					// If the format is explicitly set to "console", use console format regardless of whether the output is a terminal.
					w = zerolog.ConsoleWriter{
						Out:        os.Stderr,
						NoColor:    !term.IsTerminal(int(os.Stderr.Fd())),
						TimeFormat: "2006-01-02 15:04:05",
					}
				case JSON:
					// Default is JSON, so no need to change the writer.

				default:
					// If the format is not explicitly set to "console" or "json", auto-detect based on whether the output is a terminal.
					if term.IsTerminal(int(os.Stderr.Fd())) {
						// If the output is a terminal, use console format for better readability.
						w = zerolog.ConsoleWriter{
							Out:        os.Stderr,
							NoColor:    false,
							TimeFormat: "2006-01-02 15:04:05",
						}
					}
				}

				newL := zerolog.New(w).Level(lvl).With().Timestamp().Logger()

				return &newL, nil
			}),

			fx.Provide(func(logger *zerolog.Logger) *slog.Logger {
				return slog.New(slogzerolog.Option{
					Logger: logger,
				}.NewZerologHandler())
			}),

			fx.Invoke(slog.SetDefault),
		),

		fx.WithLogger(func(logger *zerolog.Logger) fxevent.Logger {
			log := logger.Level(logger.GetLevel()).With().Str("library.name", "fx").Logger()
			return fxeventzerolog.New(
				&log,
			)
		}),
	)
}
