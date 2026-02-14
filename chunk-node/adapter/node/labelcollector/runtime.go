package adapternodelabeler

import (
	"context"
	"runtime"

	portnode "github.com/amari/mithril/chunk-node/port/node"
)

type Runtime struct{}

var _ portnode.NodeLabelCollector = (*Runtime)(nil)

func (c *Runtime) CollectNodeLabels(_ context.Context) (map[string]string, error) {
	return map[string]string{
		"mithril.io/os":   runtime.GOOS,
		"mithril.io/arch": runtime.GOARCH,
	}, nil
}

func (c *Runtime) Close() error {
	return nil
}
