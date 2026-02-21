package clivolume

import "github.com/urfave/cli/v3"

// Command returns the `volume` command with its subcommands
func Command() *cli.Command {
	return &cli.Command{
		Name:  "volume",
		Usage: "Manage storage volumes",
		Commands: []*cli.Command{
			initCmd(),
			listCmd(),
		},
	}
}
