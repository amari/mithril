// Package adapternodelabeler provides composable wrappers for NodeLabelCollector
// that add caching, retry, and backoff behaviors.
package adapternodelabeler

import (
	"context"
	"sync"
	"time"

	portnode "github.com/amari/mithril/chunk-node/port/node"
	"github.com/cenkalti/backoff/v5"
)

type BackoffOptions struct {
	InitialInterval     time.Duration
	MaxInterval         time.Duration
	Multiplier          float64
	RandomizationFactor float64

	MaxElapsedTime time.Duration
	MaxTries       uint
}

// backoffCollector caches errors during the backoff period.
// Success is never cached.
type backoffCollector struct {
	collector portnode.NodeLabelCollector
	opts      BackoffOptions

	mu        sync.Mutex
	err       error
	retryAt   time.Time
	backoffBO *backoff.ExponentialBackOff
}

// Backoff wraps a collector with exponential backoff on errors.
//
// When the underlying collector returns an error, Backoff caches that error
// and returns it immediately for subsequent calls during the backoff period.
// After the backoff period expires, the next call will retry the underlying collector.
// The backoff duration increases exponentially with each consecutive failure.
//
// Success is never cached - every successful call invokes the underlying collector.
// On success, the backoff state is reset.
//
// Use BackoffOptions{} for sensible defaults from the backoff library.
func Backoff(collector portnode.NodeLabelCollector, opts BackoffOptions) portnode.NodeLabelCollector {
	if collector == nil {
		return nil
	}

	return &backoffCollector{
		collector: collector,
		opts:      opts,
	}
}

var _ portnode.NodeLabelCollector = (*backoffCollector)(nil)

func (b *backoffCollector) CollectNodeLabels(ctx context.Context) (map[string]string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// If we have a cached error and we're still in the backoff period, return it
	if b.err != nil && time.Now().Before(b.retryAt) {
		return nil, b.err
	}

	// If backoff period expired, reset backoff state
	if b.err != nil && time.Now().After(b.retryAt) {
		b.err = nil
		b.backoffBO = nil
	}

	labels, err := b.collector.CollectNodeLabels(ctx)
	if err != nil {
		// Initialize backoff on first error
		if b.backoffBO == nil {
			bo := backoff.NewExponentialBackOff()
			if b.opts.InitialInterval > 0 {
				bo.InitialInterval = b.opts.InitialInterval
			}
			if b.opts.MaxInterval > 0 {
				bo.MaxInterval = b.opts.MaxInterval
			}
			if b.opts.Multiplier > 0 {
				bo.Multiplier = b.opts.Multiplier
			}
			if b.opts.RandomizationFactor > 0 {
				bo.RandomizationFactor = b.opts.RandomizationFactor
			}
			bo.Reset()
			b.backoffBO = bo
		}

		// Cache error for backoff duration
		b.err = err
		b.retryAt = time.Now().Add(b.backoffBO.NextBackOff())

		return nil, err
	}

	// Success - reset backoff state, don't cache result
	b.err = nil
	b.backoffBO = nil

	return labels, nil
}

func (b *backoffCollector) Close() error {
	return b.collector.Close()
}
