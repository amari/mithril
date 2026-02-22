package adapternode

import (
	"path/filepath"

	"github.com/amari/mithril/chunk-node/port"
	portnode "github.com/amari/mithril/chunk-node/port/node"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
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
		fx.Provide(
			NewClusterPublisher,
			func(p *EtcdClusterPublisher) portnode.NodeLabelPublisher {
				return p
			},
			func(p *EtcdClusterPublisher) portvolume.VolumeIDSetLabelIndexesPublisher {
				return p
			},
		),
		fx.Provide(NewIdentityAllocator),
		fx.Provide(NewSeedGenerator),
		fx.Provide(NewNodeAnnouncer),
		fx.Provide(NewMemberResolver),
		fx.Provide(NewMemberWatchManager),
		fx.Supply(fx.Annotate(NewFileBackedNodeSeedRepository(filepath.Join(dataDir, "node_seed.json")), fx.As(new(port.NodeSeedRepository)))),
		fx.Supply(NewFileBackedNodeIdentityRepository(filepath.Join(dataDir, "node_identity.json"))),
		fx.Provide(
			func(f *fileBackedNodeIdentityRepository) port.NodeIdentityRepository {
				return f
			}, func(f *fileBackedNodeIdentityRepository) portnode.NodeIDProvider {
				return f
			},
		),
		fx.Provide(NewKubernetesLabelProvider),
		fx.Provide(fx.Annotate(func(p *KubernetesLabelProvider) portnode.NodeLabelProvider {
			return p
		}, fx.ResultTags(`group:"node-label-provider"`))),
		fx.Invoke(func(p *KubernetesLabelProvider, lc fx.Lifecycle) {
			lc.Append(fx.StartStopHook(p.Start, p.Stop))
		}),
		fx.Provide(fx.Annotate(NewRuntimeLabelProvider, fx.ResultTags(`group:"node-label-provider"`))),
	)
}
