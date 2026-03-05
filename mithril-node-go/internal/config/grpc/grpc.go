package configgrpc

import configtls "github.com/amari/mithril/mithril-node-go/internal/config/tls"

type Client struct {
	Target string            `koanf:"target"`
	TLS    *configtls.Client `koanf:"tls"`
}

type Server struct {
	Listen string            `koanf:"listen" default:"[::]:50051"`
	TLS    *configtls.Server `koanf:"tls"`
}
