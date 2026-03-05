package cli

import (
	"context"
	"time"

	adaptersetcd "github.com/amari/mithril/mithril-node-go/internal/adapters/etcd"
	adaptersfilestore "github.com/amari/mithril/mithril-node-go/internal/adapters/filestore"
	adaptersfilesystem "github.com/amari/mithril/mithril-node-go/internal/adapters/filesystem"
	adaptersgrpcserver "github.com/amari/mithril/mithril-node-go/internal/adapters/grpc/server"
	adaptershealth "github.com/amari/mithril/mithril-node-go/internal/adapters/health"
	adaptersotel "github.com/amari/mithril/mithril-node-go/internal/adapters/otel"
	adapterspebble "github.com/amari/mithril/mithril-node-go/internal/adapters/pebble"
	adapterspprof "github.com/amari/mithril/mithril-node-go/internal/adapters/pprof"
	adaptersruntime "github.com/amari/mithril/mithril-node-go/internal/adapters/runtime"
	adapterssystem "github.com/amari/mithril/mithril-node-go/internal/adapters/system"
	adapterszerolog "github.com/amari/mithril/mithril-node-go/internal/adapters/zerolog"
	applicationcommands "github.com/amari/mithril/mithril-node-go/internal/application/commands"
	applicationqueries "github.com/amari/mithril/mithril-node-go/internal/application/queries"
	applicationservices "github.com/amari/mithril/mithril-node-go/internal/application/services"
	configadvertisement "github.com/amari/mithril/mithril-node-go/internal/config/advertisement"
	configetcd "github.com/amari/mithril/mithril-node-go/internal/config/etcd"
	configgrpc "github.com/amari/mithril/mithril-node-go/internal/config/grpc"
	confighealth "github.com/amari/mithril/mithril-node-go/internal/config/health"
	configkoanf "github.com/amari/mithril/mithril-node-go/internal/config/koanf"
	configlog "github.com/amari/mithril/mithril-node-go/internal/config/log"
	configpprof "github.com/amari/mithril/mithril-node-go/internal/config/pprof"
	"github.com/creasty/defaults"
	"github.com/davecgh/go-spew/spew"
	"github.com/knadh/koanf/v2"
	"github.com/rs/zerolog"
	"github.com/urfave/cli/v3"
	"go.uber.org/fx"
)

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
				Name:     "cluster",
				Usage:    "Cluster backend type (etcd)",
				Value:    "etcd",
				OnlyOnce: true,
			},
			&cli.StringFlag{
				Name:     "etcd-endpoints",
				Usage:    "Comma-separated etcd endpoints (e.g., etcd1:2379,etcd2:2379)",
				Value:    "localhost:2379",
				OnlyOnce: true,
			},
			&cli.StringFlag{
				Name:     "etcd-prefix",
				Usage:    "Key prefix for etcd",
				Value:    "/mithril",
				OnlyOnce: true,
			},
			&cli.StringFlag{
				Name:     "etcd-username",
				Usage:    "etcd authentication username",
				OnlyOnce: true,
			},
			&cli.StringFlag{
				Name:     "etcd-password-file",
				Usage:    "Path to file containing etcd password",
				OnlyOnce: true,
			},
			&cli.StringFlag{
				Name:     "etcd-tls-ca-file",
				Usage:    "Path to etcd TLS CA certificate",
				OnlyOnce: true,
			},
			&cli.StringFlag{
				Name:     "etcd-tls-cert-file",
				Usage:    "Path to etcd TLS client certificate",
				OnlyOnce: true,
			},
			&cli.StringFlag{
				Name:     "etcd-tls-key-file",
				Usage:    "Path to etcd TLS client key",
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
			configDir := cmd.String("config-dir")

			k := koanf.New(".")
			if configDir != "" {
				if err := configkoanf.LoadDirectory(k, configDir); err != nil {
					return err
				}
			}
			if err := configkoanf.LoadCLICommand(k, cmd); err != nil {
				return err
			}
			if err := configkoanf.LoadEnv(k, "MITHRIL_"); err != nil {
				return err
			}
			if err := configkoanf.ExpandEnvVars(k); err != nil {
				return err
			}

			var serverConfig ServerConfig

			if err := k.Unmarshal("", &serverConfig); err != nil {
				return err
			}

			dataDir := cmd.String("data-dir")
			if dataDir != "" {
				serverConfig.DataDir = dataDir
			}

			if configDir != "" {
				serverConfig.ConfigDir = configDir
			}

			defaults.Set(&serverConfig)

			spew.Dump(serverConfig)

			fx.New(
				adaptersruntime.AutoMaxProcsModule(),
				fx.StopTimeout(30*time.Second),
				adaptersetcd.Module(&serverConfig.Etcd),
				adaptersfilestore.Module(),
				adaptersfilesystem.Module(serverConfig.DataDir),
				adaptersgrpcserver.Module(&serverConfig.Node.GRPC),
				adaptershealth.Module(&serverConfig.Health),
				adaptersotel.Module(),
				adapterspebble.Module(serverConfig.DataDir),
				adapterspprof.Module(&serverConfig.PProf),
				adaptersruntime.Module(),
				adapterssystem.Module(),
				adapterszerolog.Module(&serverConfig.Log),
				applicationcommands.Module(),
				applicationqueries.Module(),
				applicationservices.Module(&serverConfig.Node.Advertise),
				fx.Invoke(func(svc applicationservices.VolumeService, logger *zerolog.Logger, lc fx.Lifecycle) {
					lc.Append(fx.StartHook(func() error {
						for _, serverVolume := range serverVolumes {
							h, err := svc.AddFileStoreVolume(serverVolume)
							if err != nil {
								return err
							}

							logger.Debug().Fields(h.GetStructuredLoggingFieldsProvider().Get()).Msg("volume added")
						}

						return nil
					}))
				}),
			).Run()

			return nil
		},
	}
}

var serverVolumes []string

type ServerConfig struct {
	ConfigDir string `koanf:"-" default:"/etc/mithril.d"`
	DataDir   string `koanf:"-"`

	Cluster string            `koanf:"cluster"`
	Etcd    configetcd.Client `koanf:"etcd"`

	Health confighealth.Server `koanf:"health"`
	Log    configlog.Logger    `koanf:"log"`
	Node   struct {
		Advertise configadvertisement.AdvertisementConfig `koanf:"advertise"`
		GRPC      configgrpc.Server                       `koanf:"grpc"`
		Labels    map[string]string                       `koanf:"labels"`
		Peer      struct {
			GRPC configgrpc.Client `koanf:"grpc"`
		} `koanf:"peer"`
	} `koanf:",squash"`
	PProf   configpprof.Server `koanf:"pprof"`
	Volumes []struct {
		Labels map[string]string `koanf:"labels"`
	} `koanf:"volumes"`
}
