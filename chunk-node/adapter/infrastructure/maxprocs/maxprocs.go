package maxprocs

import (
	"github.com/rs/zerolog"
	"go.uber.org/automaxprocs/maxprocs"
	"go.uber.org/fx"
)

func Module() fx.Option {
	return fx.Module("infrastructure.maxprocs",
		fx.Invoke(initMaxprocs),
	)
}

// initMaxprocs sets up maxprocs with logging using the provided zerolog logger.
func initMaxprocs(logger *zerolog.Logger) error {
	newL := logger.With().Str("library.name", "maxprocs").Logger()

	undoF, err := maxprocs.Set(maxprocs.Logger(func(format string, a ...any) {
		newL.Debug().Msgf(format, a...)
	}))
	if err != nil {
		undoF()

		return err
	}

	return nil
}
