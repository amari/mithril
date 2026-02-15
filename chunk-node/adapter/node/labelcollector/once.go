// Package adapternodelabeler provides composable wrappers for NodeLabelCollector
// that add caching, retry, and backoff behaviors.
package adapternodelabeler

import (
	"context"
	"sync"

	portnode "github.com/amari/mithril/chunk-node/port/node"
)

// once caches the result (success or error) forever.
type once struct {
	collector portnode.NodeLabelCollector

	once   sync.Once
	labels map[string]string
	err    error
}

// Once wraps a collector and invokes it exactly once, caching the result forever.
//
// The underlying collector is called exactly once. Whether it succeeds or fails,
// the result is cached and returned for all subsequent calls. The underlying
// collector is never called again.
//
// This is useful for data that is both immutable and expected to succeed on the
// first attempt, or when failure should be permanent (e.g., fatal misconfigurations).
func Once(collector portnode.NodeLabelCollector) portnode.NodeLabelCollector {
	if collector == nil {
		return nil
	}
	return &once{collector: collector}
}

var _ portnode.NodeLabelCollector = (*once)(nil)

func (o *once) CollectNodeLabels(ctx context.Context) (map[string]string, error) {
	o.once.Do(func() {
		o.labels, o.err = o.collector.CollectNodeLabels(ctx)
	})

	return o.labels, o.err
}

func (o *once) Close() error {
	return o.collector.Close()
}
