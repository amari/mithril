package grpc

import (
	infrastructuretls "github.com/amari/mithril/chunk-node/adapter/infrastructure/tls"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type ClientConfig struct {
	TLS *infrastructuretls.ClientConfig `koanf:"tls"`
}

// DialOptions converts ClientConfig to gRPC dial options
func (cfg *ClientConfig) DialOptions() ([]grpc.DialOption, error) {
	var opts []grpc.DialOption

	if cfg.TLS != nil {
		tlsConfig, err := infrastructuretls.ClientConfigToTLSConfig(cfg.TLS)
		if err != nil {
			return nil, err
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	return opts, nil
}
