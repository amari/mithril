package node

import "context"

type NodeLabelCollector interface {
	CollectNodeLabels(ctx context.Context) (map[string]string, error)
	Close() error
}
