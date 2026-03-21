package applicationcommands

import "go.uber.org/fx"

func Module() fx.Option {
	return fx.Options(
		fx.Provide(
			NewAppendChunkCommandHandler,
			NewAppendFromChunkCommandHandler,
			NewCreateChunkCommandHandler,
			NewDeleteChunkCommandHandler,
			NewPutChunkCommandHandler,
			NewShrinkChunkToFitCommandHandler,
		),
	)
}
