package node

import (
	"github.com/amari/mithril/chunk-node/port"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/fx"
	"google.golang.org/grpc"
)

func Module(grpcURLs []string) fx.Option {
	return fx.Module("service.node",
		fx.Provide(NewNodeIdentityService),
		fx.Invoke(func(svc *NodeIdentityService, lc fx.Lifecycle) error {
			lc.Append(fx.StartHook(svc.BootstrapNodeIdentity))

			return nil
		}),
		fx.Invoke(func(announcer port.NodeAnnouncer, log *zerolog.Logger, lc fx.Lifecycle) {
			svc := NewNodeAnnouncerService(announcer, log, grpcURLs)

			lc.Append(fx.StartStopHook(svc.AnnounceNode, svc.ClearAnnouncement))
		}),
		fx.Provide(func(log *zerolog.Logger) (*MemberConnectionManager, error) {
			// TODO: configure dial options (e.g. the mTLS credentials)
			var dialOptions []grpc.DialOption

			// TODO: configure otel meter
			var meter metric.Meter

			return NewMemberConnectionManager(dialOptions, meter, log)
		}),
		fx.Provide(NewLabelService),
		fx.Invoke(func(svc *LabelService, lc fx.Lifecycle) {
			lc.Append(fx.StartStopHook(svc.Start, svc.Stop))
		}),
	)
}
