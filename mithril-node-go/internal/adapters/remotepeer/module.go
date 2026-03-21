package adaptersremotepeer

import (
	applicationservices "github.com/amari/mithril/mithril-node-go/internal/application/services"
	"go.uber.org/fx"
)

func Module() fx.Option {
	opts := []fx.Option{
		fx.Provide(
			fx.Annotate(
				NewGRPCRemotePeerChunkServiceClientProvider,
				fx.As(new(applicationservices.RemotePeerChunkServiceClientProvider)),
			),
		),
	}
	return fx.Options(opts...)
}
