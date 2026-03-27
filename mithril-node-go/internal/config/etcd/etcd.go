package configetcd

import configtls "github.com/amari/mithril/mithril-node-go/internal/config/tls"

type Client struct {
	Prefix    string            `koanf:"prefix" default:""`
	Endpoints []string          `koanf:"endpoints" default:"[\"localhost:2379\"]"`
	Username  string            `koanf:"username"`
	Password  string            `koanf:"password"`
	TLS       *configtls.Client `koanf:"tls"`
}
