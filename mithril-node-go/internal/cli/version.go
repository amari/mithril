package cli

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/urfave/cli/v3"
)

// Version information, set via ldflags at build time
var (
	version   = "dev"
	commit    = "unknown"
	buildDate = "unknown"
)

type versionInfo struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildDate string `json:"build_date"`
}

// SetVersionInfo allows setting version info from main via ldflags
func SetVersionInfo(v, c, b string) {
	version = v
	commit = c
	buildDate = b
}

func Version() *cli.Command {
	return &cli.Command{
		Name:  "version",
		Usage: "Print version information",
		Action: func(ctx context.Context, cmd *cli.Command) error {
			outputFormat := cmd.Root().String("output-format")

			v := versionInfo{
				Version:   version,
				Commit:    commit,
				BuildDate: buildDate,
			}

			switch outputFormat {
			case "json":
				enc := json.NewEncoder(cmd.Writer)
				enc.SetIndent("", "  ")
				return enc.Encode(v)
			default:
				fmt.Fprintf(cmd.Writer, "mithril-node %s\n", v.Version)
				fmt.Fprintf(cmd.Writer, "commit: %s\n", v.Commit)
				fmt.Fprintf(cmd.Writer, "built: %s\n", v.BuildDate)
				return nil
			}
		},
	}
}
