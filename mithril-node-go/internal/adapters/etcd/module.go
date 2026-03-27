package adaptersetcd

import (
	"context"
	"errors"

	configetcd "github.com/amari/mithril/mithril-node-go/internal/config/etcd"
	configtls "github.com/amari/mithril/mithril-node-go/internal/config/tls"
	"github.com/amari/mithril/mithril-node-go/internal/domain"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/fx"
)

func Module(clientCfg *configetcd.Client) fx.Option {
	if clientCfg == nil {
		return fx.Options()
	}

	options := []fx.Option{
		fx.Supply(NewPrefix(clientCfg.Prefix)),
		fx.Provide(
			NewCardShuffleScheduler,
			NewClusterMap,
			fx.Annotate(
				NewNodeClaimRegistry,
				fx.As(new(domain.NodeClaimRegistry)),
			),
			fx.Annotate(
				NewNodeLabelPublisher,
				fx.As(new(domain.NodeLabelPublisher)),
			),
			fx.Annotate(
				func(m *ClusterMap) *ClusterMap {
					return m
				},
				fx.As(new(domain.NodePeerResolver)),
			),
			fx.Annotate(
				NewNodePresencePublisher,
				fx.As(new(domain.NodePresencePublisher)),
			),
			fx.Annotate(
				NewVolumeLabelPublisher,
				fx.As(new(domain.VolumeLabelPublisher)),
			),
			func(lc fx.Lifecycle) (*clientv3.Client, error) {
				clientCtx, clientCancelF := context.WithCancel(context.Background())

				client := clientv3.NewCtxClient(clientCtx)

				cfg := clientv3.Config{
					Context:   clientCtx,
					Endpoints: clientCfg.Endpoints,
					Username:  client.Username,
					Password:  client.Password,
				}
				if clientCfg.TLS != nil {
					tlsConfig, err := configtls.TLSConfigWithClient(clientCfg.TLS)
					if err != nil {
						clientCancelF()

						return nil, err
					}

					cfg.TLS = tlsConfig
				}

				lc.Append(fx.Hook{
					OnStart: func(ctx context.Context) error {
						realClient, err := clientv3.New(cfg)
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
			},
		),
		fx.Invoke(
			func(s *CardShuffleScheduler, lc fx.Lifecycle) {
				lc.Append(fx.StartStopHook(s.Start, s.Stop))
			},
			func(m *ClusterMap, lc fx.Lifecycle) {
				lc.Append(fx.StartStopHook(m.Start, m.Stop))
			},
		),
	}

	return fx.Options(options...)
}
