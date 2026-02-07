package transport

import (
	"github.com/amari/mithril/chunk-node/transport/grpc"
	"go.uber.org/fx"
)

func Module() fx.Option {
	return fx.Module("transport",
		grpc.Module(),
	)
}
