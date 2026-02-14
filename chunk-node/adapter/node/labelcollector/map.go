package adapternodelabeler

import (
	"context"

	portnode "github.com/amari/mithril/chunk-node/port/node"
)

type Map map[string]string

var _ portnode.NodeLabelCollector = (Map)(nil)

func (m Map) CollectNodeLabels(_ context.Context) (map[string]string, error) {
	return m, nil
}

func (m Map) Close() error {
	return nil
}
