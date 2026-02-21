package cli

import (
	"context"
	"path/filepath"
	"time"

	adapterchunk "github.com/amari/mithril/chunk-node/adapter/chunk"
	infraetcd "github.com/amari/mithril/chunk-node/adapter/infrastructure/etcd"
	infragrpc "github.com/amari/mithril/chunk-node/adapter/infrastructure/grpc"
	infrahealthcheck "github.com/amari/mithril/chunk-node/adapter/infrastructure/healthcheck"
	infralog "github.com/amari/mithril/chunk-node/adapter/infrastructure/log"
	"github.com/amari/mithril/chunk-node/adapter/infrastructure/maxprocs"
	infraotel "github.com/amari/mithril/chunk-node/adapter/infrastructure/otel"
	infrapprof "github.com/amari/mithril/chunk-node/adapter/infrastructure/pprof"
	infratls "github.com/amari/mithril/chunk-node/adapter/infrastructure/tls"
	adapternode "github.com/amari/mithril/chunk-node/adapter/node"
	adapterremotechunknodegrpc "github.com/amari/mithril/chunk-node/adapter/remotechunknode/grpc"
	adaptervolume "github.com/amari/mithril/chunk-node/adapter/volume"
	"github.com/amari/mithril/chunk-node/config"
	"github.com/amari/mithril/chunk-node/service"
	"github.com/amari/mithril/chunk-node/transport"
	"github.com/creasty/defaults"
	"github.com/knadh/koanf/v2"
	"github.com/urfave/cli/v3"
	"go.uber.org/fx"
)

var serverVolumes []string

func Server() *cli.Command {
	return &cli.Command{
		Name:  "server",
		Usage: "Start the chunk node server",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "data-dir",
				Usage:    "Directory path for runtime data",
				Value:    "/var/lib/mithril",
				OnlyOnce: true,
			},
			&cli.StringFlag{
				Name:     "advertise-grpc-urls",
				Usage:    "Comma-separated list of gRPC URLs to advertise to peers",
				OnlyOnce: true,
			},
			&cli.StringFlag{
				Name:     "grpc-listen",
				Usage:    "gRPC server listen address",
				Value:    "0.0.0.0:50051",
				OnlyOnce: true,
			},
			&cli.StringFlag{
				Name:     "grpc-tls-ca-file",
				Usage:    "Path to CA certificate for gRPC TLS",
				OnlyOnce: true,
			},
			&cli.StringFlag{
				Name:     "grpc-tls-cert-file",
				Usage:    "Path to server certificate for gRPC TLS",
				OnlyOnce: true,
			},
			&cli.StringFlag{
				Name:     "grpc-tls-key-file",
				Usage:    "Path to server key for gRPC TLS",
				OnlyOnce: true,
			},
			&cli.StringFlag{
				Name:     "peer-tls-ca-file",
				Usage:    "Path to CA certificate for peer TLS",
				OnlyOnce: true,
			},
			&cli.StringFlag{
				Name:     "peer-tls-cert-file",
				Usage:    "Path to certificate for peer TLS",
				OnlyOnce: true,
			},
			&cli.StringFlag{
				Name:     "peer-tls-key-file",
				Usage:    "Path to key for peer TLS",
				OnlyOnce: true,
			},
			&cli.BoolFlag{
				Name:     "health-enable",
				Usage:    "Enable health check server",
				Value:    true,
				OnlyOnce: true,
			},
			&cli.StringFlag{
				Name:     "health-listen",
				Usage:    "Health check server listen address",
				Value:    "0.0.0.0:8080",
				OnlyOnce: true,
			},
			&cli.BoolFlag{
				Name:     "pprof-enable",
				Usage:    "Enable pprof server for profiling",
				Value:    false,
				OnlyOnce: true,
			},
			&cli.StringFlag{
				Name:     "pprof-listen",
				Usage:    "pprof server listen address",
				Value:    "0.0.0.0:6060",
				OnlyOnce: true,
			},
		},
		Arguments: []cli.Argument{
			&cli.StringArgs{
				Name:        "volumes",
				Destination: &serverVolumes,
				UsageText:   "VOLUME_PATH...",
				Min:         0,
				Max:         -1,
				Config: cli.StringConfig{
					TrimSpace: true,
				},
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			cfg, err := parseServerConfig(cmd)
			if err != nil {
				return err
			}
			return runServer(ctx, cfg)
		},
	}
}

type ServerConfig struct {
	config.ClusterConfig `koanf:",squash"`
	config.NodeConfig    `koanf:",squash"`
	Log                  infralog.Config `koanf:"log"`

	DataDir string   `koanf:"-" defaults:"/var/lib/mithril"`
	Volumes []string `koanf:"-"`
}

func parseServerConfig(cmd *cli.Command) (*ServerConfig, error) {
	k := koanf.New(".")

	if configDir := cmd.String("config-dir"); configDir != "" {
		clusterDirPath := filepath.Join(configDir, "cluster.d")
		if err := config.LoadDirectory(k, clusterDirPath); err != nil {
			return nil, err
		}

		nodeDirPath := filepath.Join(configDir, "node.d")
		if err := config.LoadDirectory(k, nodeDirPath); err != nil {
			return nil, err
		}
	}

	if err := config.LoadCLICommand(k, cmd); err != nil {
		return nil, err
	}

	if err := config.LoadEnv(k, "MITHRIL_"); err != nil {
		return nil, err
	}

	if err := config.ExpandEnvVars(k); err != nil {
		return nil, err
	}

	var serverConfig ServerConfig

	if err := defaults.Set(&serverConfig); err != nil {
		return nil, err
	}

	if err := k.Unmarshal("", &serverConfig); err != nil {
		return nil, err
	}

	if dataDir := cmd.String("data-dir"); dataDir != "" {
		serverConfig.DataDir = dataDir
	}

	serverConfig.Volumes = serverVolumes

	return &serverConfig, nil
}

func runServer(_ context.Context, cfg *ServerConfig) error {
	fx.New(
		fx.StopTimeout(30*time.Second),
		maxprocs.Module(),

		adapterModule(cfg),
		infraModule(cfg),
		serviceModule(cfg),
	).Run()

	return nil
}

func adapterModule(cfg *ServerConfig) fx.Option {
	return fx.Options(
		adapterchunk.Module(cfg.DataDir),
		adapternode.Module(&adapternode.Config{
			Advertise: adapternode.ConfigAdvertise{
				GRPC: adapternode.ConfigAdvertiseGRPC{
					URLs: cfg.Advertise.GRPC.URLs,
				},
			},
			Labels: cfg.Labels,
		}, cfg.DataDir),
		adaptervolume.Module(cfg.DataDir),
		adapterremotechunknodegrpc.Module(),
		transport.Module(),
	)
}

func infraModule(cfg *ServerConfig) fx.Option {
	options := []fx.Option{
		infraetcd.Module(&infraetcd.Config{
			Endpoints: cfg.Etcd.Endpoints,
			TLS:       tlsClientConfig(cfg.Etcd.TLS),
		}),
		infragrpc.ServerModule(&infragrpc.ServerConfig{
			Address: cfg.GRPC.Listen,
			TLS:     tlsServerConfig(cfg.GRPC.TLS),
		}),
		infragrpc.ResolverModule(),
		infralog.Module(&cfg.Log),
		infraotel.Module(),
	}

	if cfg.Health.Enable {
		options = append(options, infrahealthcheck.Module(&infrahealthcheck.Config{
			Address: cfg.Health.Listen,
		}))
	}

	if cfg.PProf.Enable {
		options = append(options, infrapprof.Module(&infrapprof.Config{
			Enabled: true,
			Address: cfg.PProf.Listen,
		}))
	}

	return fx.Options(
		options...,
	)
}

func serviceModule(cfg *ServerConfig) fx.Option {
	return fx.Options(
		service.Module(cfg.Volumes, cfg.Advertise.GRPC.URLs),
	)
}

func tlsClientConfig(cfg *config.TLSClientConfig) *infratls.ClientConfig {
	if cfg == nil {
		return nil
	}

	return &infratls.ClientConfig{
		CertFile:     cfg.Cert.File,
		KeyFile:      cfg.Key.File,
		ServerCAFile: cfg.CA.File,
	}
}

func tlsServerConfig(cfg *config.TLSServerConfig) *infratls.ServerConfig {
	if cfg == nil {
		return nil
	}

	return &infratls.ServerConfig{
		CertFile:     cfg.Cert.File,
		KeyFile:      cfg.Key.File,
		ClientCAFile: cfg.CA.File,
	}
}
