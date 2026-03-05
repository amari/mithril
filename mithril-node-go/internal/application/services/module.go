package applicationservices

import (
	configadvertisement "github.com/amari/mithril/mithril-node-go/internal/config/advertisement"
	"github.com/amari/mithril/mithril-node-go/internal/domain"
	"go.uber.org/fx"
)

func Module(advertisementCfg *configadvertisement.AdvertisementConfig) fx.Option {
	options := []fx.Option{
		fx.Invoke(
			func(svc *NodeSeedService, lc fx.Lifecycle) {
				lc.Append(fx.StartHook(svc.Start))
			},
			func(svc *NodeClaimService, lc fx.Lifecycle) {
				lc.Append(fx.StartHook(svc.Start))
			},
			func(svc *NodeLabelService, lc fx.Lifecycle) {
				lc.Append(fx.StartStopHook(svc.Start, svc.Stop))
			},
			func(svc *NodePresenceService, lc fx.Lifecycle) {
				lc.Append(fx.StartStopHook(svc.Start, svc.Stop))
			},
			func(svc VolumeService, lc fx.Lifecycle) {
				lc.Append(fx.StartStopHook(svc.Start, svc.Stop))
			},
		),
		fx.Provide(
			func() NodeAdvertisedGRPCURLs {
				return NodeAdvertisedGRPCURLs(advertisementCfg.GRPC.URLs)
			},
			fx.Annotate(NewChunkIDGenerator, fx.As(new(domain.ChunkIDGenerator))),
			NewNodeClaimService,
			fx.Annotate(NewNodeLabelService, fx.ParamTags(`group:"node-label-sources"`)),
			NewNodePresenceService,
			NewNodeSeedService,
			fx.Annotate(NewRoundRobinVolumeChooser, fx.As(new(domain.VolumeChooser)), fx.ParamTags(`optional:"true"`)),
			NewVolumeService,
		),
	}

	return fx.Options(options...)
}
