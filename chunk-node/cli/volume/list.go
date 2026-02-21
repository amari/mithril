package clivolume

import (
	"context"

	"github.com/urfave/cli/v3"
)

func listCmd() *cli.Command {
	return &cli.Command{
		Name:  "list",
		Usage: "List all configured volumes",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "data-dir",
				Usage:    "Directory path for runtime data",
				Value:    "/var/lib/mithril",
				OnlyOnce: true,
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			cfg, err := parseListConfig(cmd)
			if err != nil {
				return err
			}
			return runList(ctx, cfg)
		},
	}
}

type ListConfig struct {
	DataDir      string
	OutputFormat string
	ConfigDir    string
}

func parseListConfig(cmd *cli.Command) (*ListConfig, error) {
	return &ListConfig{
		DataDir:      cmd.String("data-dir"),
		OutputFormat: cmd.Root().String("output-format"),
		ConfigDir:    cmd.Root().String("config-dir"),
	}, nil
}

func runList(ctx context.Context, cfg *ListConfig) error {
	// TODO: Implement volume list logic
	return nil
}
