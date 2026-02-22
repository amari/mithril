package adapternode

import (
	"context"
	"runtime"

	portnode "github.com/amari/mithril/chunk-node/port/node"
)

type RuntimeLabelProvider struct{}

var _ portnode.NodeLabelProvider = (*RuntimeLabelProvider)(nil)

func NewRuntimeLabelProvider() portnode.NodeLabelProvider {
	return &RuntimeLabelProvider{}
}

func (p *RuntimeLabelProvider) GetNodeLabels() map[string]string {
	return map[string]string{
		"mithril.io/os":   runtime.GOOS,
		"mithril.io/arch": runtime.GOARCH,
	}
}

func (p *RuntimeLabelProvider) Watch(watchCtx context.Context) <-chan struct{} {
	ch := make(chan struct{})
	close(ch)

	return ch
}
