// Package adapternodelabeler provides composable wrappers for NodeLabelCollector
// that add caching, retry, and backoff behaviors.
package adapternodelabeler

import (
	"context"
	"sync"

	portnode "github.com/amari/mithril/chunk-node/port/node"
)

// first caches the first successful result forever.
// Errors are not cached.
type first struct {
	collector portnode.NodeLabelCollector

	mu     sync.Mutex
	labels map[string]string
}

// First wraps a collector and caches the first successful result forever.
//
// When the underlying collector returns successfully, First caches the result
// and returns it immediately for all subsequent calls. The underlying collector
// is never called again after the first success.
//
// Errors are not cached - the next call will retry the underlying collector immediately.
// This is useful for immutable data that may require retries to fetch initially.
func First(collector portnode.NodeLabelCollector) portnode.NodeLabelCollector {
	if collector == nil {
		return nil
	}
	return &first{collector: collector}
}

var _ portnode.NodeLabelCollector = (*first)(nil)

func (f *first) CollectNodeLabels(ctx context.Context) (map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.labels != nil {
		return f.labels, nil
	}

	labels, err := f.collector.CollectNodeLabels(ctx)
	if err != nil {
		return nil, err
	}

	f.labels = labels
	return f.labels, nil
}

func (f *first) Close() error {
	return f.collector.Close()
}
