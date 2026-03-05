package adaptersruntime

import (
	"github.com/rs/zerolog"
	"go.uber.org/automaxprocs/maxprocs"
	"go.uber.org/fx"
)

func AutoMaxProcsModule() fx.Option {
	return fx.Invoke(func(logger *zerolog.Logger) error {
		undoF, err := maxprocs.Set(maxprocs.Logger(func(format string, a ...any) {
			logger.Info().Msgf(format, a...)
		}))
		if err != nil {
			undoF()

			return err
		}

		return nil
	})
}
