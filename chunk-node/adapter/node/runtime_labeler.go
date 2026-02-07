package node

import (
	"context"
	"runtime"

	"github.com/amari/mithril/chunk-node/port"
)

type RuntimeNodeLabeler struct{}

var _ port.NodeLabeler = (*RuntimeNodeLabeler)(nil)

func NewRuntimeNodeLabeler() port.NodeLabeler {
	return &RuntimeNodeLabeler{}
}

func (nl *RuntimeNodeLabeler) Labels(_ context.Context) (map[string]string, error) {
	return map[string]string{
		"mithril.io/os":   runtime.GOOS,
		"mithril.io/arch": runtime.GOARCH,
	}, nil
}
