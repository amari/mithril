package cli

import (
	cliconfig "github.com/amari/mithril/mithril-node-go/internal/cli/config"
	clivolume "github.com/amari/mithril/mithril-node-go/internal/cli/volume"
	"github.com/urfave/cli/v3"
)

func Root() *cli.Command {
	return &cli.Command{
		Name:  "mithril-node",
		Usage: "Mithril distributed chunk storage node",
		Flags: GlobalFlags(),
		Commands: []*cli.Command{
			Init(),
			Server(),
			Version(),
			cliconfig.Command(),
			clivolume.Command(),
		},
	}
}

// GlobalFlags are flags available to all commands (appear before subcommand)
func GlobalFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:     "output-format",
			Usage:    "Output format (json, text)",
			Value:    "text",
			OnlyOnce: true,
		},
		&cli.StringFlag{
			Name:        "config-dir",
			Usage:       "Path to configuration directory",
			DefaultText: "/etc/mithril.d",
			OnlyOnce:    true,
		},
		&cli.StringFlag{
			Name:     "log-format",
			Usage:    "Log output format (json, console)",
			OnlyOnce: true,
		},
		&cli.StringFlag{
			Name:     "log-level",
			Usage:    "Log level (debug, info, warn, error)",
			Value:    "info",
			OnlyOnce: true,
		},
	}
}
