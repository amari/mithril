package node

import (
	"context"

	"github.com/amari/mithril/chunk-node/port"
)

type StaticNodeLabeler map[string]string

var _ port.NodeLabeler = (StaticNodeLabeler)(nil)

func NewStaticNodeLabeler(labels map[string]string) port.NodeLabeler {
	return StaticNodeLabeler(labels)
}

func (nl StaticNodeLabeler) Labels(_ context.Context) (map[string]string, error) {
	return nl, nil
}
