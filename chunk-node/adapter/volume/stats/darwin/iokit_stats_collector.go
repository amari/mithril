//go:build darwin
// +build darwin

package darwin

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	iokit "github.com/amari/mithril/chunk-node/darwin"
	"github.com/amari/mithril/chunk-node/domain"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
	"github.com/rs/zerolog"
)

// IOKitBlockDeviceStatsCollector polls IOKit block storage driver statistics in its own goroutine
type IOKitBlockDeviceStatsCollector struct {
	path    string
	log     *zerolog.Logger
	nowFunc func() time.Time
	driver  *iokit.IOBlockStorageDriver

	mu           sync.RWMutex
	epoch        uint64
	cachedSample domain.Sample[*domain.BlockDeviceStats]

	// Track if a collection goroutine is in progress to avoid spawning unbounded goroutines
	collecting atomic.Bool

	cancel context.CancelFunc
	done   chan struct{}
}

var _ portvolume.VolumeStatsCollector[*domain.BlockDeviceStats] = (*IOKitBlockDeviceStatsCollector)(nil)

func NewIOKitBlockDeviceStatsCollector(
	ctx context.Context,
	path string,
	pollInterval time.Duration,
	log *zerolog.Logger,
	nowFunc func() time.Time,
) (*IOKitBlockDeviceStatsCollector, error) {
	// Resolve path → IOMedia → IOBlockStorageDriver
	media, err := iokit.IOMediaFromPath(path)
	if err != nil {
		return nil, fmt.Errorf("failed to get IOMedia from path: %w", err)
	}
	defer media.Close()

	log.Debug().Str("bsdName", media.BSDName()).Msg("Resolved IOMedia from path")

	driver, err := iokit.GetBlockStorageDriverFromMedia(media)
	if err != nil {
		return nil, fmt.Errorf("failed to get IOBlockStorageDriver: %w", err)
	}

	log.Debug().Str("className", driver.ClassName()).Msg("Resolved IOBlockStorageDriver from IOMedia")

	pollCtx, cancel := context.WithCancel(ctx)

	collector := &IOKitBlockDeviceStatsCollector{
		path:    path,
		log:     log,
		nowFunc: nowFunc,
		driver:  driver,
		cancel:  cancel,
		done:    make(chan struct{}),
	}

	// Start poll loop in constructor
	go collector.pollLoop(pollCtx, pollInterval)

	return collector, nil
}

func (c *IOKitBlockDeviceStatsCollector) pollLoop(ctx context.Context, interval time.Duration) {
	defer close(c.done)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Poll immediately on start
	c.collect()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.collect()
		}
	}
}

func (c *IOKitBlockDeviceStatsCollector) collect() {
	// Check if a collection is already in progress
	if !c.collecting.CompareAndSwap(false, true) {
		// Already collecting, skip this poll to avoid spawning unbounded goroutines
		c.log.Warn().Msg("IOKit collection still in progress, skipping poll")

		// Still increment epoch to show we attempted collection
		c.mu.Lock()
		c.epoch++
		c.mu.Unlock()

		return
	}

	// Increment epoch BEFORE attempting IOKit query
	c.mu.Lock()
	c.epoch++
	epoch := c.epoch
	c.mu.Unlock()

	// Bounded timeout for IOKit
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Perform IOKit query in a goroutine so we can timeout
	type result struct {
		stats *domain.BlockDeviceStats
		err   error
	}
	resultCh := make(chan result, 1)

	go func() {
		defer c.collecting.Store(false) // Clear flag when done

		stats, err := c.queryIOKit()
		resultCh <- result{stats: stats, err: err}
	}()

	var stats *domain.BlockDeviceStats
	var err error

	select {
	case res := <-resultCh:
		stats = res.stats
		err = res.err
	case <-ctx.Done():
		err = ctx.Err()
		// Note: The goroutine with IOKit query is still blocked, but we move on
		// The collecting flag will stay true, preventing new goroutines
	}

	// Update cached sample
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cachedSample.Epoch = epoch
	c.cachedSample.Error = err

	if err == nil {
		c.cachedSample.Value = stats
		c.cachedSample.Time = c.nowFunc()
	}
	// If err != nil (timeout or IOKit error), Time is NOT updated → Epoch advances but Time frozen
}

func (c *IOKitBlockDeviceStatsCollector) queryIOKit() (*domain.BlockDeviceStats, error) {
	// Read raw statistics dictionary from IOKit
	rawStats, err := c.driver.ReadRawStatistics()
	if err != nil {
		return nil, fmt.Errorf("failed to read IOKit statistics: %w", err)
	}

	// Parse the map[string]any into BlockDeviceStatsApple
	stats := &domain.BlockDeviceStatsApple{}

	if v, ok := rawStats["Bytes (Read)"].(int64); ok {
		stats.BytesRead = v
	}
	if v, ok := rawStats["Bytes (Write)"].(int64); ok {
		stats.BytesWritten = v
	}
	if v, ok := rawStats["Latency Time (Read)"].(int64); ok {
		stats.LatentReadTime = v
	}
	if v, ok := rawStats["Latency Time (Write)"].(int64); ok {
		stats.LatentWriteTime = v
	}
	if v, ok := rawStats["Errors (Read)"].(int64); ok {
		stats.ReadErrors = v
	}
	if v, ok := rawStats["Retries (Read)"].(int64); ok {
		stats.ReadRetries = v
	}
	if v, ok := rawStats["Operations (Read)"].(int64); ok {
		stats.Reads = v
	}
	if v, ok := rawStats["Total Time (Read)"].(int64); ok {
		stats.TotalReadTime = v
	}
	if v, ok := rawStats["Total Time (Write)"].(int64); ok {
		stats.TotalWriteTime = v
	}
	if v, ok := rawStats["Errors (Write)"].(int64); ok {
		stats.WriteErrors = v
	}
	if v, ok := rawStats["Retries (Write)"].(int64); ok {
		stats.WriteRetries = v
	}
	if v, ok := rawStats["Operations (Write)"].(int64); ok {
		stats.Writes = v
	}

	return &domain.BlockDeviceStats{
		Apple: stats,
	}, nil
}

// CollectVolumeStats implements VolumeStatsCollector interface
func (c *IOKitBlockDeviceStatsCollector) CollectVolumeStats() (domain.Sample[*domain.BlockDeviceStats], error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cachedSample, nil
}

func (c *IOKitBlockDeviceStatsCollector) Close() error {
	if c.cancel != nil {
		c.cancel()
		<-c.done
	}

	if c.driver != nil {
		return c.driver.Close()
	}

	return nil
}
