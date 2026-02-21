package cliconfig

import "github.com/urfave/cli/v3"

// Command returns the `config` command with its subcommands
func Command() *cli.Command {
	return &cli.Command{
		Name:  "config",
		Usage: "Configuration management",
		Commands: []*cli.Command{
			validateCmd(),
		},
	}
}
