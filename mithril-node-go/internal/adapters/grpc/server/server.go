package adaptersgrpcserver

import (
	"go.uber.org/fx"
	"google.golang.org/grpc"
)

type ServerParams struct {
	fx.In
}

type ServerResult struct {
	fx.Out

	Server *grpc.Server
}

func NewServer(params ServerParams) ServerResult {
	return ServerResult{}
}

func InvokeServer(lc fx.Lifecycle) error {
	return nil
}
