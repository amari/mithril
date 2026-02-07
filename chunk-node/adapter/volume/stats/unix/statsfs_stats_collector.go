package unix

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/amari/mithril/chunk-node/domain"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
	"github.com/rs/zerolog"
	"golang.org/x/sys/unix"
)

// StatfsSpaceUtilizationStatsCollector polls statfs in its own goroutine
type StatfsSpaceUtilizationStatsCollector struct {
	file *os.File
	log  *zerolog.Logger

	mu           sync.RWMutex
	epoch        uint64
	cachedSample domain.Sample[*domain.SpaceUtilizationStats]

	// Track if a collection goroutine is in progress to avoid spawning unbounded goroutines
	collecting atomic.Bool

	cancel context.CancelFunc
	done   chan struct{}
}

var _ portvolume.VolumeStatsCollector[*domain.SpaceUtilizationStats] = (*StatfsSpaceUtilizationStatsCollector)(nil)

func NewStatfsSpaceUtilizationStatsCollector(
	ctx context.Context,
	path string,
	pollInterval time.Duration,
	log *zerolog.Logger,
) (*StatfsSpaceUtilizationStatsCollector, error) {
	file, err := os.OpenFile(path, unix.O_DIRECTORY|unix.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}

	pollCtx, cancel := context.WithCancel(ctx)

	collector := &StatfsSpaceUtilizationStatsCollector{
		file:   file,
		log:    log,
		cancel: cancel,
		done:   make(chan struct{}),
	}

	// Start poll loop in constructor
	go collector.pollLoop(pollCtx, pollInterval)

	return collector, nil
}

func (c *StatfsSpaceUtilizationStatsCollector) pollLoop(ctx context.Context, interval time.Duration) {
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

func (c *StatfsSpaceUtilizationStatsCollector) collect() {
	// Check if a collection is already in progress
	if !c.collecting.CompareAndSwap(false, true) {
		// Already collecting, skip this poll to avoid spawning unbounded goroutines
		c.log.Warn().Msg("statfs collection still in progress, skipping poll")

		// Still increment epoch to show we attempted collection
		c.mu.Lock()
		c.epoch++
		c.mu.Unlock()

		return
	}

	// Increment epoch BEFORE attempting syscall
	c.mu.Lock()
	c.epoch++
	epoch := c.epoch
	c.mu.Unlock()

	// Bounded timeout for statfs
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Perform statfs in a goroutine so we can timeout
	type result struct {
		stat unix.Statfs_t
		err  error
	}
	resultCh := make(chan result, 1)

	go func() {
		defer c.collecting.Store(false) // Clear flag when done (success or still blocked)

		var stat unix.Statfs_t
		err := unix.Fstatfs(int(c.file.Fd()), &stat)
		resultCh <- result{stat: stat, err: err}
	}()

	var stat unix.Statfs_t
	var err error

	select {
	case res := <-resultCh:
		stat = res.stat
		err = res.err
	case <-ctx.Done():
		err = ctx.Err()
		// Note: The goroutine with Fstatfs is still blocked, but we move on
		// The collecting flag will stay true, preventing new goroutines for being spawned
	}

	// Update cached sample
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cachedSample.Epoch = epoch
	c.cachedSample.Error = err

	if err == nil {
		c.cachedSample.Value = &domain.SpaceUtilizationStats{
			TotalBytes: int64(stat.Blocks) * int64(stat.Bsize),
			FreeBytes:  int64(stat.Bavail) * int64(stat.Bsize),
			UsedBytes:  int64(stat.Blocks-stat.Bfree) * int64(stat.Bsize),
		}
		c.cachedSample.Time = time.Now()
	}
	// If err != nil (timeout or syscall error), Time is NOT updated → Epoch advances but Time frozen
}

// CollectVolumeStats implements VolumeStatsCollector interface
func (c *StatfsSpaceUtilizationStatsCollector) CollectVolumeStats() (domain.Sample[*domain.SpaceUtilizationStats], error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cachedSample, nil
}

func (c *StatfsSpaceUtilizationStatsCollector) Close() error {
	if c.cancel != nil {
		c.cancel()
		<-c.done
	}
	return c.file.Close()
}
