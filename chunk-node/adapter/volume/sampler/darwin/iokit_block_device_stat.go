//go:build darwin
// +build darwin

package samplerdarwin

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	iokit "github.com/amari/mithril/chunk-node/darwin"
	"github.com/amari/mithril/chunk-node/domain"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
)

// IOKitBlockDeviceStatSamplerOptions contains configuration options for IOKitBlockDeviceStatSampler.
type IOKitBlockDeviceStatSamplerOptions struct {
	// PollInterval is the interval between IOKit statistics collections.
	// If zero, defaults to 5 seconds.
	PollInterval time.Duration
}

func (o IOKitBlockDeviceStatSamplerOptions) pollInterval() time.Duration {
	if o.PollInterval == 0 {
		return 5 * time.Second
	}
	return o.PollInterval
}

// IOKitBlockDeviceStatSampler collects block device statistics from IOKit on Darwin.
type IOKitBlockDeviceStatSampler struct {
	nowFunc func() time.Time

	driver *iokit.IOBlockStorageDriver

	mu           sync.RWMutex
	epoch        uint64
	cachedSample domain.Sample[*domain.BlockDeviceStats]

	// collecting tracks if a collection is in progress to avoid overlapping collections.
	collecting atomic.Bool

	cancelFunc func()
	doneCh     chan struct{}
}

var _ portvolume.Sampler[*domain.BlockDeviceStats] = (*IOKitBlockDeviceStatSampler)(nil)

// NewIOKitBlockDeviceStatSamplerWithPath creates a new sampler for the block device at the given filesystem path.
func NewIOKitBlockDeviceStatSamplerWithPath(path string, opts IOKitBlockDeviceStatSamplerOptions) (*IOKitBlockDeviceStatSampler, error) {
	media, err := iokit.IOMediaFromPath(path)
	if err != nil {
		return nil, fmt.Errorf("failed to get IOMedia from path: %w", err)
	}
	defer media.Close()

	return newIOKitBlockDeviceStatSampler(media, opts)
}

// NewIOKitBlockDeviceStatSamplerWithBSDName creates a new sampler for the block device with the given BSD name.
func NewIOKitBlockDeviceStatSamplerWithBSDName(bsdName string, opts IOKitBlockDeviceStatSamplerOptions) (*IOKitBlockDeviceStatSampler, error) {
	media, err := iokit.IOMediaFromBSDName(bsdName)
	if err != nil {
		return nil, fmt.Errorf("failed to get IOMedia from bsdName: %w", err)
	}
	defer media.Close()

	return newIOKitBlockDeviceStatSampler(media, opts)
}

func newIOKitBlockDeviceStatSampler(media *iokit.IOMedia, opts IOKitBlockDeviceStatSamplerOptions) (*IOKitBlockDeviceStatSampler, error) {
	driver, err := iokit.GetBlockStorageDriverFromMedia(media)
	if err != nil {
		return nil, fmt.Errorf("failed to get IOBlockStorageDriver: %w", err)
	}

	doneCh := make(chan struct{})
	stopCh := make(chan struct{})

	sampler := &IOKitBlockDeviceStatSampler{
		nowFunc:    time.Now,
		driver:     driver,
		cancelFunc: func() { close(stopCh) },
		doneCh:     doneCh,
	}

	go sampler.pollLoop(stopCh, opts.pollInterval())

	return sampler, nil
}

func (s *IOKitBlockDeviceStatSampler) pollLoop(stopCh <-chan struct{}, interval time.Duration) {
	defer close(s.doneCh)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Poll immediately on start
	s.collect()

	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			s.collect()
		}
	}
}

func (s *IOKitBlockDeviceStatSampler) collect() {
	// Check if a collection is already in progress
	if !s.collecting.CompareAndSwap(false, true) {
		// Already collecting, skip this poll to avoid overlapping collections.
		return
	}
	defer s.collecting.Store(false)

	// Increment epoch BEFORE attempting IOKit query
	s.mu.Lock()
	s.epoch++
	epoch := s.epoch
	s.mu.Unlock()

	// Query IOKit synchronously
	stats, err := s.queryIOKit()

	// Update cached sample
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cachedSample.Epoch = epoch
	s.cachedSample.Error = err

	if err == nil {
		s.cachedSample.Value = stats
		s.cachedSample.Time = s.nowFunc()
	}
	// If err != nil, Time is NOT updated → Epoch advances but Time frozen
}

func (s *IOKitBlockDeviceStatSampler) queryIOKit() (*domain.BlockDeviceStats, error) {
	// Read raw statistics dictionary from IOKit
	rawStats, err := s.driver.ReadRawStatistics()
	if err != nil {
		return nil, fmt.Errorf("failed to read IOKit statistics: %w", err)
	}

	// Parse the map[string]any into BlockDeviceStatsApple
	stats := &domain.BlockDeviceStatsApple{
		BytesRead:       getInt64(rawStats, "Bytes (Read)"),
		BytesWritten:    getInt64(rawStats, "Bytes (Write)"),
		LatentReadTime:  getInt64(rawStats, "Latency Time (Read)"),
		LatentWriteTime: getInt64(rawStats, "Latency Time (Write)"),
		ReadErrors:      getInt64(rawStats, "Errors (Read)"),
		ReadRetries:     getInt64(rawStats, "Retries (Read)"),
		Reads:           getInt64(rawStats, "Operations (Read)"),
		TotalReadTime:   getInt64(rawStats, "Total Time (Read)"),
		TotalWriteTime:  getInt64(rawStats, "Total Time (Write)"),
		WriteErrors:     getInt64(rawStats, "Errors (Write)"),
		WriteRetries:    getInt64(rawStats, "Retries (Write)"),
		Writes:          getInt64(rawStats, "Operations (Write)"),
	}

	return &domain.BlockDeviceStats{
		Apple: stats,
	}, nil
}

func getInt64(m map[string]any, key string) int64 {
	if v, ok := m[key].(int64); ok {
		return v
	}
	return 0
}

// GetSample returns a copy of the current cached sample.
// The returned sample is safe to use without synchronization.
func (s *IOKitBlockDeviceStatSampler) GetSample() domain.Sample[*domain.BlockDeviceStats] {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Return a copy to avoid data races and external modification
	sample := s.cachedSample
	return sample
}

// Close stops the polling loop and releases resources.
func (s *IOKitBlockDeviceStatSampler) Close() error {
	s.cancelFunc()

	<-s.doneCh // Wait for poll loop to exit

	return s.driver.Close()
}
