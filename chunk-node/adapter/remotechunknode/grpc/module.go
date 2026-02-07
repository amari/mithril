package grpc

import (
	"github.com/amari/mithril/chunk-node/port/remotechunknode"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func Module() fx.Option {
	return fx.Module("adapter.remotechunknode.grpc",
		fx.Provide(func(meter metric.Meter, log *zerolog.Logger) (remotechunknode.RemoteChunkClient, error) {
			// TODO: TLS and other dial options
			var dialOptions []grpc.DialOption

			dialOptions = append(dialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))

			return NewRemoteChunkClient(dialOptions, meter, log)
		}),
	)
}
