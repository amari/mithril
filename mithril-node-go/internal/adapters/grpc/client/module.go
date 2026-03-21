package adaptersgrpcclient

import (
	"go.uber.org/fx"
	"google.golang.org/grpc/resolver"
)

func Module() fx.Option {
	opts := []fx.Option{
		fx.Provide(
			NewMithrilNodeResolverBuilder,
		),
		fx.Invoke(
			func(b *MithrilNodeResolverBuilder, lc fx.Lifecycle) {
				lc.Append(fx.StartStopHook(b.Start, b.Stop))
				lc.Append(fx.StartHook(func() {
					resolver.Register(b)
				}))
			},
		),
	}
	return fx.Options(opts...)
}
