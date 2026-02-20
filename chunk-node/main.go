package main

import (
	"context"
	"fmt"
	"os"
	"time"

	adapterchunk "github.com/amari/mithril/chunk-node/adapter/chunk"
	"github.com/amari/mithril/chunk-node/adapter/infrastructure/etcd"
	"github.com/amari/mithril/chunk-node/adapter/infrastructure/grpc"
	"github.com/amari/mithril/chunk-node/adapter/infrastructure/healthcheck"
	"github.com/amari/mithril/chunk-node/adapter/infrastructure/log"
	"github.com/amari/mithril/chunk-node/adapter/infrastructure/maxprocs"
	"github.com/amari/mithril/chunk-node/adapter/infrastructure/otel"
	"github.com/amari/mithril/chunk-node/adapter/infrastructure/pprof"
	adapternode "github.com/amari/mithril/chunk-node/adapter/node"
	remotechunknodegrpc "github.com/amari/mithril/chunk-node/adapter/remotechunknode/grpc"
	"github.com/amari/mithril/chunk-node/adapter/volume"
	"github.com/amari/mithril/chunk-node/service"
	"github.com/amari/mithril/chunk-node/transport"
	"github.com/knadh/koanf/v2"
	"github.com/urfave/cli/v3"
	"go.uber.org/fx"
)

func main() {
	err := buildRootCommand().Run(context.Background(), os.Args)
	if err != nil {
		fmt.Println(err.Error())
	}
}

func buildRootCommand() *cli.Command {
	rootCmd := &cli.Command{
		Name:  "chunk-store",
		Usage: "Chunk Store Service",
		Commands: []*cli.Command{
			buildServerCommand(),
		},
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "config",
				Usage:    "The Chunkstore will load its configuration from this file. The path may be absolute or relative.",
				Aliases:  []string{"c"},
				OnlyOnce: true,
			},
			&cli.StringFlag{
				Name:     "config-dir",
				Usage:    "Path to a directory containing configuration files to be loaded.",
				Aliases:  []string{"d"},
				OnlyOnce: true,
			},
			&cli.StringFlag{
				Name:        "data-dir",
				Usage:       "Directory path for managing runtime data",
				DefaultText: "/var/lib/mithril",
				OnlyOnce:    true,
			},
			&cli.StringFlag{
				Name:        "log-format",
				Usage:       "Log output format (json or text)",
				DefaultText: "json",
				OnlyOnce:    true,
			},
			&cli.StringFlag{
				Name:        "log-level",
				Usage:       "Log level (debug, info, warn, error, fatal, panic)",
				DefaultText: "info",
				Value:       "info",
				OnlyOnce:    true,
			},
		},
	}

	return rootCmd
}

var argServerVolumes []string

func buildServerCommand() *cli.Command {
	return &cli.Command{
		Name:  "server",
		Usage: "Run the Chunk Store server",
		Action: func(ctx context.Context, cmd *cli.Command) error {
			k := koanf.New(".")

			if flagValue := cmd.String("config"); flagValue != "" {
				if err := LoadConfigFromFile(k, flagValue); err != nil {
					return err
				}
			}

			if flagValue := cmd.String("config-dir"); flagValue != "" {
				if err := LoadConfigFromDirectory(k, flagValue); err != nil {
					return err
				}
			}

			if err := LoadConfigFromEnv(k, "MITHRIL_"); err != nil {
				return err
			}

			if err := LoadConfigFromCLI(k, cmd.Root()); err != nil {
				return err
			}

			if err := expandKoanfValuesWithGetenv(k); err != nil {
				return err
			}

			var cfg Config
			if err := k.Unmarshal("", &cfg); err != nil {
				return err
			}

			dataDir := "/var/lib/mithril"
			if arg := cmd.String("data-dir"); arg != "" {
				dataDir = arg
			}

			fx.New(
				fx.StopTimeout(1*time.Minute),
				log.Module(&cfg.Log),
				maxprocs.Module(),
				otel.Module(),

				etcd.Module(&cfg.Etcd),
				grpc.ResolverModule(),
				grpc.ServerModule(&cfg.GRPC),
				healthcheck.Module(&cfg.HealthCheck),
				pprof.Module(&cfg.Pprof),
				remotechunknodegrpc.Module(),

				adaptervolume.Module(dataDir),

				adapterchunk.Module(dataDir),
				adapternode.Module(&cfg.Node, dataDir),
				transport.Module(),
				service.Module(argServerVolumes, cfg.Node.Advertise.GRPC.URLs),
			).Run()

			return nil
		},
		Arguments: []cli.Argument{
			&cli.StringArgs{
				Name:        "volumes",
				Destination: &argServerVolumes,
				UsageText:   "VOLUME_PATH...",
				Min:         0,
				Max:         -1,
				Config: cli.StringConfig{
					TrimSpace: true,
				},
			},
		},
	}
}
