package grpc

import "go.uber.org/fx"

func ResolverModule() fx.Option {
	return fx.Module("infrastructure.grpc.resolver",
		fx.Invoke(NewChunkNodeResolverBuilder),
	)
}
