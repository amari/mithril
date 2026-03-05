package adaptersruntime

import (
	"context"
	"runtime"

	"github.com/amari/mithril/mithril-node-go/internal/domain"
)

type NodeLabelSource struct{}

func NewNodeLabelSource() *NodeLabelSource {
	return &NodeLabelSource{}
}

var _ domain.NodeLabelSource = (*NodeLabelSource)(nil)

func (*NodeLabelSource) Read() map[string]string {
	return map[string]string{
		"mithril.io/os":   runtime.GOOS,
		"mithril.io/arch": runtime.GOARCH,
	}
}

func (*NodeLabelSource) Watch(watchCtx context.Context) <-chan struct{} {
	ch := make(chan struct{})
	close(ch)

	return ch
}
