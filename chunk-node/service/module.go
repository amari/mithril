package service

import (
	"github.com/amari/mithril/chunk-node/service/chunk"
	"github.com/amari/mithril/chunk-node/service/node"
	"github.com/amari/mithril/chunk-node/service/volume"
	"go.uber.org/fx"
)

func Module(directoryVolumePaths []string, grpcURLs []string) fx.Option {
	return fx.Options(
		fx.Module("service"),
		chunk.Module(),
		node.Module(grpcURLs),
		volume.Module(directoryVolumePaths),
	)
}
