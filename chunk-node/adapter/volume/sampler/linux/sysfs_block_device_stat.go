//go:build linux
// +build linux

package samplerlinux

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/amari/mithril/chunk-node/domain"
	linuxpkg "github.com/amari/mithril/chunk-node/linux"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
)

// SysfsBlockDeviceStatSamplerOptions contains configuration options for SysfsBlockDeviceStatSampler.
type SysfsBlockDeviceStatSamplerOptions struct {
	// PollInterval is the interval between sysfs statistics collections.
	// If zero, defaults to 5 seconds.
	PollInterval time.Duration
}

func (o SysfsBlockDeviceStatSamplerOptions) pollInterval() time.Duration {
	if o.PollInterval == 0 {
		return 5 * time.Second
	}
	return o.PollInterval
}

// SysfsBlockDeviceStatSampler collects block device statistics from sysfs on Linux.
type SysfsBlockDeviceStatSampler struct {
	nowFunc func() time.Time

	statPath string

	mu           sync.RWMutex
	epoch        uint64
	cachedSample domain.Sample[*domain.BlockDeviceStats]

	// collecting tracks if a collection is in progress to avoid overlapping collections.
	collecting atomic.Bool

	cancelFunc func()
	doneCh     chan struct{}
}

var _ portvolume.Sampler[*domain.BlockDeviceStats] = (*SysfsBlockDeviceStatSampler)(nil)

// NewSysfsBlockDeviceStatSamplerWithPath creates a new sampler for the block device
// underlying the given filesystem path.
func NewSysfsBlockDeviceStatSamplerWithPath(path string, opts SysfsBlockDeviceStatSamplerOptions) (*SysfsBlockDeviceStatSampler, error) {
	blockDevice, err := linuxpkg.ResolveBlockDevice(path)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve block device: %w", err)
	}

	return newSysfsBlockDeviceStatSampler(blockDevice.StatPath(), opts)
}

// NewSysfsBlockDeviceStatSamplerWithDeviceName creates a new sampler for the block device
// with the given name (e.g., "sda", "nvme0n1").
func NewSysfsBlockDeviceStatSamplerWithDeviceName(deviceName string, opts SysfsBlockDeviceStatSamplerOptions) (*SysfsBlockDeviceStatSampler, error) {
	statPath := filepath.Join("/sys/block", deviceName, "stat")

	return newSysfsBlockDeviceStatSampler(statPath, opts)
}

func newSysfsBlockDeviceStatSampler(statPath string, opts SysfsBlockDeviceStatSamplerOptions) (*SysfsBlockDeviceStatSampler, error) {
	// Verify the stat file exists
	if _, err := os.Stat(statPath); err != nil {
		return nil, fmt.Errorf("failed to access sysfs stat file: %w", err)
	}

	doneCh := make(chan struct{})
	stopCh := make(chan struct{})

	sampler := &SysfsBlockDeviceStatSampler{
		nowFunc:    time.Now,
		statPath:   statPath,
		cancelFunc: func() { close(stopCh) },
		doneCh:     doneCh,
	}

	go sampler.pollLoop(stopCh, opts.pollInterval())

	return sampler, nil
}

func (s *SysfsBlockDeviceStatSampler) pollLoop(stopCh <-chan struct{}, interval time.Duration) {
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

func (s *SysfsBlockDeviceStatSampler) collect() {
	// Check if a collection is already in progress
	if !s.collecting.CompareAndSwap(false, true) {
		// Already collecting, skip this poll to avoid overlapping collections.
		return
	}
	defer s.collecting.Store(false)

	// Increment epoch BEFORE attempting sysfs read
	s.mu.Lock()
	s.epoch++
	epoch := s.epoch
	s.mu.Unlock()

	// Query sysfs synchronously
	stats, err := s.querySysfs()

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

func (s *SysfsBlockDeviceStatSampler) querySysfs() (*domain.BlockDeviceStats, error) {
	file, err := os.Open(s.statPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open sysfs stat file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return nil, fmt.Errorf("failed to read sysfs stat file: %w", err)
		}
		return nil, fmt.Errorf("empty sysfs stat file")
	}

	fields := strings.Fields(scanner.Text())

	// The stat file has 17 fields (as of kernel 5.5+), but older kernels may have fewer.
	// Fields are space-separated integers in this order:
	//  0: read I/Os completed
	//  1: read I/Os merged
	//  2: sectors read
	//  3: time spent reading (ms)
	//  4: write I/Os completed
	//  5: write I/Os merged
	//  6: sectors written
	//  7: time spent writing (ms)
	//  8: I/Os currently in progress
	//  9: time spent doing I/Os (ms)
	// 10: weighted time spent doing I/Os (ms)
	// 11: discard I/Os completed (kernel 4.18+)
	// 12: discard I/Os merged
	// 13: sectors discarded
	// 14: time spent discarding (ms)
	// 15: flush I/Os completed (kernel 5.5+)
	// 16: time spent flushing (ms)

	stats := &domain.BlockDeviceStatsLinux{
		Reads:            parseInt64(fields, 0),
		MergedReads:      parseInt64(fields, 1),
		SectorsRead:      parseInt64(fields, 2),
		ReadTicks:        parseInt64(fields, 3),
		Writes:           parseInt64(fields, 4),
		MergedWrites:     parseInt64(fields, 5),
		SectorsWritten:   parseInt64(fields, 6),
		WriteTicks:       parseInt64(fields, 7),
		InFlight:         parseInt64(fields, 8),
		IOTicks:          parseInt64(fields, 9),
		TimeInQueue:      parseInt64(fields, 10),
		Discards:         parseInt64(fields, 11),
		MergedDiscards:   parseInt64(fields, 12),
		SectorsDiscarded: parseInt64(fields, 13),
		DiscardTicks:     parseInt64(fields, 14),
		Flushes:          parseInt64(fields, 15),
		FlushTicks:       parseInt64(fields, 16),
	}

	return &domain.BlockDeviceStats{
		Linux: stats,
	}, nil
}

func parseInt64(fields []string, index int) int64 {
	if index >= len(fields) {
		return 0
	}
	v, _ := strconv.ParseInt(fields[index], 10, 64)
	return v
}

// GetSample returns a copy of the current cached sample.
// The returned sample is safe to use without synchronization.
func (s *SysfsBlockDeviceStatSampler) GetSample() domain.Sample[*domain.BlockDeviceStats] {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Return a copy to avoid data races and external modification
	sample := s.cachedSample
	return sample
}

// Close stops the polling loop and releases resources.
func (s *SysfsBlockDeviceStatSampler) Close() error {
	s.cancelFunc()

	<-s.doneCh // Wait for poll loop to exit

	return nil
}
