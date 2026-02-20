package node

import "context"

type NodeLabelProvider interface {
	GetNodeLabels() map[string]string

	Watch(watchCtx context.Context) <-chan struct{}
}
