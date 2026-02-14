//go:build unix
// +build unix

package samplerunix

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/port/volume"
	"golang.org/x/sys/unix"
)

// StatfsSpaceUtilizationStatSamplerOptions contains configuration options for StatfsSpaceUtilizationStatSampler.
type StatfsSpaceUtilizationStatSamplerOptions struct {
	// PollInterval is the interval between statfs collections.
	// If zero, defaults to 5 seconds.
	PollInterval time.Duration
}

func (o StatfsSpaceUtilizationStatSamplerOptions) pollInterval() time.Duration {
	if o.PollInterval == 0 {
		return 5 * time.Second
	}
	return o.PollInterval
}

// StatfsSpaceUtilizationStatSampler collects space utilization statistics via statfs on Unix systems.
type StatfsSpaceUtilizationStatSampler struct {
	nowFunc func() time.Time

	file *os.File

	mu           sync.RWMutex
	epoch        uint64
	cachedSample domain.Sample[*domain.SpaceUtilizationStats]

	// collecting tracks if a collection is in progress to avoid overlapping collections.
	collecting atomic.Bool

	cancelFunc func()
	doneCh     chan struct{}
}

var _ volume.Sampler[*domain.SpaceUtilizationStats] = (*StatfsSpaceUtilizationStatSampler)(nil)

// NewStatfsSpaceUtilizationStatSampler creates a new sampler for the filesystem at the given path.
func NewStatfsSpaceUtilizationStatSampler(path string, opts StatfsSpaceUtilizationStatSamplerOptions) (*StatfsSpaceUtilizationStatSampler, error) {
	file, err := os.OpenFile(path, unix.O_DIRECTORY|unix.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open path: %w", err)
	}

	doneCh := make(chan struct{})
	stopCh := make(chan struct{})

	sampler := &StatfsSpaceUtilizationStatSampler{
		nowFunc:    time.Now,
		file:       file,
		cancelFunc: func() { close(stopCh) },
		doneCh:     doneCh,
	}

	go sampler.pollLoop(stopCh, opts.pollInterval())

	return sampler, nil
}

func (s *StatfsSpaceUtilizationStatSampler) pollLoop(stopCh <-chan struct{}, interval time.Duration) {
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

func (s *StatfsSpaceUtilizationStatSampler) collect() {
	// Check if a collection is already in progress
	if !s.collecting.CompareAndSwap(false, true) {
		// Already collecting, skip this poll to avoid overlapping collections.
		return
	}
	defer s.collecting.Store(false)

	// Increment epoch BEFORE attempting statfs
	s.mu.Lock()
	s.epoch++
	epoch := s.epoch
	s.mu.Unlock()

	// Query statfs synchronously
	stats, err := s.queryStatfs()

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

func (s *StatfsSpaceUtilizationStatSampler) queryStatfs() (*domain.SpaceUtilizationStats, error) {
	var stat unix.Statfs_t
	if err := unix.Fstatfs(int(s.file.Fd()), &stat); err != nil {
		return nil, fmt.Errorf("failed to statfs: %w", err)
	}

	return &domain.SpaceUtilizationStats{
		TotalBytes: int64(stat.Blocks) * int64(stat.Bsize),
		FreeBytes:  int64(stat.Bavail) * int64(stat.Bsize),
		UsedBytes:  int64(stat.Blocks-stat.Bfree) * int64(stat.Bsize),
	}, nil
}

// GetSample returns a copy of the current cached sample.
// The returned sample is safe to use without synchronization.
func (s *StatfsSpaceUtilizationStatSampler) GetSample() domain.Sample[*domain.SpaceUtilizationStats] {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Return a copy to avoid data races and external modification
	sample := s.cachedSample
	return sample
}

// Close stops the polling loop and releases resources.
func (s *StatfsSpaceUtilizationStatSampler) Close() error {
	s.cancelFunc()

	<-s.doneCh // Wait for poll loop to exit

	return s.file.Close()
}
