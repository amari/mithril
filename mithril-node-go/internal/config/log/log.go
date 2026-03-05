package configlog

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/rs/zerolog"
)

type Logger struct {
	// Level specifies the minimum log level to output. Supported values are "debug", "info", "warn", "error", "fatal", and "panic".
	Level Level `koanf:"level" json:"level" yaml:"level"`

	// Format specifies the log output format. Supported values are "json" and "console".
	Format Format `koanf:"format" json:"format" yaml:"format"`
}

type Format string

const (
	Console Format = "console"
	JSON    Format = "json"
)

func (f *Format) UnmarshalText(text []byte) error {
	switch strings.ToLower(string(text)) {
	case string(Console), string(JSON), "":
		*f = Format(text)
		return nil
	default:
		return fmt.Errorf("invalid log format: %q", text)
	}
}

func (f *Format) UnmarshalJSON(data []byte) error {
	// Remove quotes if present
	str := strings.Trim(string(data), `"`)
	return f.UnmarshalText([]byte(str))
}

type Level string

const (
	Trace Level = "trace"
	Debug Level = "debug"
	Info  Level = "info"
	Warn  Level = "warn"
	Error Level = "error"
	Fatal Level = "fatal"
	Panic Level = "panic"
)

func (l Level) ZerologLevel() (zerolog.Level, error) {
	switch l {
	case Trace:
		return zerolog.TraceLevel, nil
	case Debug:
		return zerolog.DebugLevel, nil
	case Info:
		return zerolog.InfoLevel, nil
	case Warn:
		return zerolog.WarnLevel, nil
	case Error:
		return zerolog.ErrorLevel, nil
	case Fatal:
		return zerolog.FatalLevel, nil
	case Panic:
		return zerolog.PanicLevel, nil
	default:
		return zerolog.InfoLevel, fmt.Errorf("invalid log level: %q", l)
	}
}

func (l Level) SlogLevel() (slog.Level, error) {
	switch l {
	case Trace:
		return slog.LevelDebug, nil
	case Debug:
		return slog.LevelDebug, nil
	case Info:
		return slog.LevelInfo, nil
	case Warn:
		return slog.LevelWarn, nil
	case Error:
		return slog.LevelError, nil
	case Fatal:
		return slog.LevelError, nil // slog does not have a Fatal level, so we map it to Error.
	case Panic:
		return slog.LevelError, nil // slog does not have a Panic level, so we map it to Error.
	default:
		return slog.LevelInfo, fmt.Errorf("invalid log level: %q", l)
	}
}

func (l *Level) UnmarshalText(text []byte) error {
	switch strings.ToLower(string(text)) {
	case string(Debug), string(Info), string(Warn), string(Error), string(Fatal), string(Panic):
		*l = Level(text)
		return nil
	default:
		return fmt.Errorf("invalid log level: %q", text)
	}
}

func (l *Level) UnmarshalJSON(data []byte) error {
	// Remove quotes if present
	str := strings.Trim(string(data), `"`)
	return l.UnmarshalText([]byte(str))
}
