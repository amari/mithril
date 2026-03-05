package applicationcommands

import "go.uber.org/fx"

func Module() fx.Option {
	return fx.Options(
		fx.Provide(
			NewAppendChunkCommandHandler,
			NewCreateChunkCommandHandler,
			NewDeleteChunkCommandHandler,
			NewPutChunkCommandHandler,
			NewShrinkChunkToFitCommandHandler,
		),
	)
}
