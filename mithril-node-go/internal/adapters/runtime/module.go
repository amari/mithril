package adaptersruntime

import (
	"github.com/amari/mithril/mithril-node-go/internal/domain"
	"go.uber.org/fx"
)

func Module() fx.Option {
	return fx.Provide(
		fx.Annotate(
			NewNodeLabelSource,
			fx.As(new(domain.NodeLabelSource)),
			fx.ResultTags(`group:"node-label-sources"`),
		),
	)
}
