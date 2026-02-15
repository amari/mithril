// Package adapternodelabeler provides composable wrappers for NodeLabelCollector
// that add caching, retry, and backoff behaviors.
package adapternodelabeler

import (
	"context"
	"sync"
	"time"

	portnode "github.com/amari/mithril/chunk-node/port/node"
)

type DebounceOptions struct {
	TTL time.Duration
}

const defaultDebounceTTL = 30 * time.Second

// debounce caches successful results for a TTL.
// Errors are not cached.
type debounce struct {
	collector portnode.NodeLabelCollector
	opts      DebounceOptions

	mu        sync.Mutex
	labels    map[string]string
	expiresAt time.Time
}

// Debounce wraps a collector with time-based caching for successful results.
//
// When the underlying collector returns successfully, Debounce caches the result
// and returns it immediately for subsequent calls until the TTL expires.
// Concurrent calls during a fetch are deduplicated - only one call to the
// underlying collector is made.
//
// Errors are not cached - the next call will retry the underlying collector immediately.
//
// Use DebounceOptions{} for the default TTL of 30 seconds.
func Debounce(collector portnode.NodeLabelCollector, opts DebounceOptions) portnode.NodeLabelCollector {
	if collector == nil {
		return nil
	}
	return &debounce{
		collector: collector,
		opts:      opts,
	}
}

var _ portnode.NodeLabelCollector = (*debounce)(nil)

func (d *debounce) CollectNodeLabels(ctx context.Context) (map[string]string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.labels != nil && time.Now().Before(d.expiresAt) {
		return d.labels, nil
	}

	labels, err := d.collector.CollectNodeLabels(ctx)
	if err != nil {
		return nil, err
	}

	ttl := d.opts.TTL
	if ttl <= 0 {
		ttl = defaultDebounceTTL
	}

	d.labels = labels
	d.expiresAt = time.Now().Add(ttl)

	return d.labels, nil
}

func (d *debounce) Close() error {
	return d.collector.Close()
}
