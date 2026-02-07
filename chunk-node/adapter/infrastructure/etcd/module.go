package etcd

import (
	"context"
	"errors"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/fx"
)

func Module(cfg *Config) fx.Option {
	return fx.Module("infrastructure.etcd",
		fx.Provide(func(lc fx.Lifecycle) (*clientv3.Client, error) {
			clientCtx, clientCancelF := context.WithCancel(context.Background())

			client := clientv3.NewCtxClient(clientCtx)

			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					etcdCfg, err := cfg.EtcdConfig()
					if err != nil {
						return err
					}
					etcdCfg.Context = clientCtx

					realClient, err := clientv3.New(etcdCfg)
					if err != nil {
						return err
					}

					client.Auth = realClient.Auth
					client.Cluster = realClient.Cluster
					client.KV = realClient.KV
					client.Lease = realClient.Lease
					client.Maintenance = realClient.Maintenance
					client.Password = realClient.Password
					client.Username = realClient.Username
					client.Watcher = realClient.Watcher

					return nil
				},
				OnStop: func(ctx context.Context) error {
					defer clientCancelF()

					if err := client.Close(); err != nil {
						if !errors.Is(err, context.Canceled) {
							return err
						}
					}

					return nil
				},
			})

			return client, nil
		}),
	)
}
