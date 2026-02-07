package chunk

import (
	"github.com/amari/mithril/chunk-node/service/chunk/command"
	"github.com/amari/mithril/chunk-node/service/chunk/query"
	"go.uber.org/fx"
)

func Module() fx.Option {
	return fx.Options(
		fx.Module("service.chunk"),
		command.Module(),
		query.Module(),
	)
}
