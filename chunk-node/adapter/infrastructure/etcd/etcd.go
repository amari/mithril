package etcd

import (
	infrastructuretls "github.com/amari/mithril/chunk-node/adapter/infrastructure/tls"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Config struct {
	Endpoints []string                        `koanf:"endpoints"`
	TLS       *infrastructuretls.ClientConfig `koanf:"tls"`
}

func (cfg *Config) Validate() error {
	if cfg.TLS != nil {
		if err := cfg.TLS.Validate(); err != nil {
			return err
		}
	}

	return nil
}

// EtcdConfig converts Config to etcd clientv3.Config
func (cfg *Config) EtcdConfig() (clientv3.Config, error) {
	etcdCfg := clientv3.Config{
		Endpoints: cfg.Endpoints,
	}

	if cfg.TLS != nil {
		tlsConfig, err := infrastructuretls.ClientConfigToTLSConfig(cfg.TLS)
		if err != nil {
			return etcdCfg, err
		}

		etcdCfg.TLS = tlsConfig
	}

	return etcdCfg, nil
}
