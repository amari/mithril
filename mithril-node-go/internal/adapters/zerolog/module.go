package adapterszerolog

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	configlog "github.com/amari/mithril/mithril-node-go/internal/config/log"
	"github.com/rs/zerolog"
	slogzerolog "github.com/samber/slog-zerolog/v2"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
	"golang.org/x/term"
)

func Module(loggerCfg *configlog.Logger) fx.Option {
	options := []fx.Option{
		fx.Provide(func() (*zerolog.Logger, error) {
			lvl := zerolog.InfoLevel
			var w io.Writer = os.Stderr
			var err error

			if loggerCfg.Level != "" {
				lvl, err = loggerCfg.Level.ZerologLevel()
				if err != nil {
					return nil, fmt.Errorf("invalid log level %q: %w", loggerCfg.Level, err)
				}
			}

			switch loggerCfg.Format {
			case configlog.Console:
				noColor := !term.IsTerminal(int(os.Stderr.Fd()))

				// If the format is explicitly set to "console", use console format regardless of whether the output is a terminal.
				w = zerolog.ConsoleWriter{
					Out:        os.Stderr,
					NoColor:    noColor,
					TimeFormat: "2006-01-02 15:04:05",
					PartsOrder: []string{
						zerolog.TimestampFieldName,
						zerolog.LevelFieldName,
						zerolog.MessageFieldName,
						zerolog.CallerFieldName,
					},
					FieldsExclude: []string{
						"moduletrace",
						"stacktrace",
					},
					FormatCaller: consoleWriterCallerFormatter(noColor),
				}
			case configlog.JSON:
				// Default is JSON, so no need to change the writer.

			default:
				// If the format is not explicitly set to "console" or "json", auto-detect based on whether the output is a terminal.
				if term.IsTerminal(int(os.Stderr.Fd())) {
					// If the output is a terminal, use console format for better readability.
					w = zerolog.ConsoleWriter{
						Out:        os.Stderr,
						NoColor:    false,
						TimeFormat: "2006-01-02 15:04:05",
						PartsOrder: []string{
							zerolog.TimestampFieldName,
							zerolog.LevelFieldName,
							zerolog.MessageFieldName,
							zerolog.CallerFieldName,
						},
						FieldsExclude: []string{
							"moduletrace",
							"stacktrace",
						},
						FormatCaller: consoleWriterCallerFormatter(false),
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

		fx.WithLogger(func(logger *zerolog.Logger) fxevent.Logger {
			log := logger.Level(logger.GetLevel()).With().Logger()
			return NewFXLogger(
				&log,
			)
		}),
	}

	return fx.Options(options...)
}

func consoleWriterCallerFormatter(noColor bool) func(i any) string {
	return func(i any) string {
		var c string
		if cc, ok := i.(string); ok {
			c = cc
		}
		if len(c) > 0 {
			if cwd, err := os.Getwd(); err == nil {
				if rel, err := filepath.Rel(cwd, c); err == nil {
					c = rel
				}
			}
			c = colorize("caller=", colorCyan, noColor) + c
		}
		return c
	}
}

// colorize returns the string s wrapped in ANSI code c, unless disabled is true or c is 0.
func colorize(s any, c int, disabled bool) string {
	e := os.Getenv("NO_COLOR")
	if e != "" || c == 0 {
		disabled = true
	}

	if disabled {
		return fmt.Sprintf("%s", s)
	}
	return fmt.Sprintf("\x1b[%dm%v\x1b[0m", c, s)
}

const (
	colorBlack = iota + 30
	colorRed
	colorGreen
	colorYellow
	colorBlue
	colorMagenta
	colorCyan
	colorWhite

	colorBold     = 1
	colorDarkGray = 90
)
