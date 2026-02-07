package node

import (
	"path/filepath"

	"github.com/amari/mithril/chunk-node/port"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
)

type Config struct {
	Advertise ConfigAdvertise   `koanf:"advertise"`
	Labels    map[string]string `koanf:"labels"`
}

type ConfigAdvertise struct {
	GRPC ConfigAdvertiseGRPC `koanf:"grpc"`
}

type ConfigAdvertiseGRPC struct {
	URLs []string `koanf:"urls"`
}

func Module(cfg *Config, dataDir string) fx.Option {
	return fx.Module("node",
		fx.Provide(NewIdentityAllocator),
		fx.Provide(NewSeedGenerator),
		fx.Provide(NewNodeLabelPublisher),
		fx.Provide(NewNodeAnnouncer),
		fx.Provide(NewMemberResolver),
		fx.Provide(NewMemberWatchManager),
		fx.Supply(fx.Annotate(NewFileBackedNodeSeedRepository(filepath.Join(dataDir, "node_seed.json")), fx.As(new(port.NodeSeedRepository)))),
		fx.Supply(fx.Annotate(NewFileBackedNodeIdentityRepository(filepath.Join(dataDir, "node_identity.json")), fx.As(new(port.NodeIdentityRepository)))),
		fx.Provide(func(log *zerolog.Logger) port.NodeLabeler {
			labelers := []port.NodeLabeler{
				NewStaticNodeLabeler(cfg.Labels),
				NewRuntimeNodeLabeler(),
			}

			if k8sLabeler, err := KubernetesInClusterNodeLabeler(); err != nil {
				log.Warn().Err(err).Msg("Failed to create Kubernetes node labeler, skipping")
			} else {
				labelers = append(labelers, k8sLabeler)
			}

			return NewChainNodeLabeler(labelers...)
		}),
	)
}
