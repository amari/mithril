package command

import "go.uber.org/fx"

func Module() fx.Option {
	return fx.Module("service.chunk.command",
		fx.Provide(NewCreateChunkHandler),
		fx.Provide(NewPutChunkHandler),
		fx.Provide(NewAppendChunkHandler),
		fx.Provide(NewAppendFromChunkHandler),
		fx.Provide(NewDeleteChunkHandler),
		fx.Provide(NewShrinkChunkHandler),
	)
}
