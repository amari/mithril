package log

import (
	"fmt"
	"io"
	"log/slog"
	"os"

	fxeventzerolog "github.com/amari/fxevent-zerolog"
	"github.com/rs/zerolog"
	slogzerolog "github.com/samber/slog-zerolog/v2"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
)

type Config struct {
	Level  string `koanf:"level" json:"level" yaml:"level"`
	Pretty bool   `koanf:"pretty" json:"pretty" yaml:"pretty"`
}

func Module(cfg *Config) fx.Option {
	return fx.Options(
		fx.Module("infrastructure.log",
			fx.Provide(func() (*zerolog.Logger, error) {
				lvl := zerolog.InfoLevel
				var w io.Writer = os.Stderr
				var err error

				if cfg.Level != "" {
					lvl, err = zerolog.ParseLevel(cfg.Level)
					if err != nil {
						return nil, fmt.Errorf("invalid log level %q: %w", cfg.Level, err)
					}
				}

				if cfg.Pretty {
					// Use console writer for pretty output
					w = zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "2006-01-02 15:04:05"}
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
			log := logger.Level(zerolog.InfoLevel).With().Str("library.name", "fx").Logger()
			return fxeventzerolog.New(
				&log,
			)
		}),
	)
}
