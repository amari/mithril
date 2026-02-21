package cli

import (
	"context"

	"github.com/urfave/cli/v3"
)

func Init() *cli.Command {
	return &cli.Command{
		Name:  "init",
		Usage: "Initialize the node data directory and cluster membership",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "data-dir",
				Usage:    "Directory path for runtime data",
				Value:    "/var/lib/mithril",
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
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			cfg, err := parseInitConfig(cmd)
			if err != nil {
				return err
			}
			return runInit(ctx, cfg)
		},
	}
}

type InitConfig struct {
	DataDir          string
	Cluster          string
	EtcdEndpoints    string
	EtcdPrefix       string
	EtcdUsername     string
	EtcdPasswordFile string
	EtcdTLSCA        string
	EtcdTLSCert      string
	EtcdTLSKey       string
}

func parseInitConfig(cmd *cli.Command) (*InitConfig, error) {
	return &InitConfig{
		DataDir:          cmd.String("data-dir"),
		Cluster:          cmd.String("cluster"),
		EtcdEndpoints:    cmd.String("etcd-endpoints"),
		EtcdPrefix:       cmd.String("etcd-prefix"),
		EtcdUsername:     cmd.String("etcd-username"),
		EtcdPasswordFile: cmd.String("etcd-password-file"),
		EtcdTLSCA:        cmd.String("etcd-tls-ca-file"),
		EtcdTLSCert:      cmd.String("etcd-tls-cert-file"),
		EtcdTLSKey:       cmd.String("etcd-tls-key-file"),
	}, nil
}

func runInit(ctx context.Context, cfg *InitConfig) error {
	// TODO: Implement init logic
	return nil
}
