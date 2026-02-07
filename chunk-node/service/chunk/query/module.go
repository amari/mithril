package query

import "go.uber.org/fx"

func Module() fx.Option {
	return fx.Module("service.chunk.query",
		fx.Provide(NewReadChunkHandler),
		fx.Provide(NewStatChunkHandler),
	)
}
