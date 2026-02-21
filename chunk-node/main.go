package main

import (
	"context"
	"fmt"
	"os"

	"github.com/amari/mithril/chunk-node/cli"
)

func main() {
	if err := cli.Root().Run(context.Background(), os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}
