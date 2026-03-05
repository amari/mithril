package adapterssystem

import (
	"time"

	"github.com/amari/mithril/mithril-node-go/internal/domain"
	"go.uber.org/fx"
)

func Module() fx.Option {
	options := []fx.Option{
		fx.Supply(time.Now),
		fx.Provide(
			fx.Annotate(
				NewNodeSeedGenerator,
				fx.As(new(domain.NodeSeedGenerator)),
			),
		),
	}

	return fx.Options(options...)
}
