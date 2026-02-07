package grpc

import (
	chunkstorev1 "github.com/amari/mithril/gen/go/proto/chunkstore/v1"
	chunkv1 "github.com/amari/mithril/gen/go/proto/mithril/chunk/v1"
	"go.uber.org/fx"
	"google.golang.org/grpc"
)

func Module() fx.Option {
	return fx.Module("transport.grpc",
		fx.Provide(NewChunkStoreChunkServiceServer),
		fx.Provide(NewChunkServiceServer),
		fx.Invoke(func(server *grpc.Server, srv *ChunkStoreChunkServiceServer) {
			chunkstorev1.RegisterChunkServiceServer(server, srv)
		}),
		fx.Invoke(func(server *grpc.Server, srv *ChunkServiceServer) {
			chunkv1.RegisterChunkServiceServer(server, srv)
		}),
	)
}
