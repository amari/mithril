//go:build linux
// +build linux

package linux

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/amari/mithril/chunk-node/domain"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
	"github.com/rs/zerolog"
	"golang.org/x/sys/unix"
)

// SysfsBlockDeviceStatsCollector polls /sys/block in its own goroutine
type SysfsBlockDeviceStatsCollector struct {
	path     string
	statPath string // /sys/block/<device>/stat
	log      *zerolog.Logger
	nowFunc  func() time.Time

	mu           sync.RWMutex
	epoch        uint64
	cachedSample domain.Sample[*domain.BlockDeviceStats]

	// Track if a collection goroutine is in progress to avoid spawning unbounded goroutines
	collecting atomic.Bool

	cancel context.CancelFunc
	done   chan struct{}
}

var _ portvolume.VolumeStatsCollector[*domain.BlockDeviceStats] = (*SysfsBlockDeviceStatsCollector)(nil)

func NewSysfsBlockDeviceStatsCollector(
	ctx context.Context,
	path string,
	pollInterval time.Duration,
	log *zerolog.Logger,
	nowFunc func() time.Time,
) (*SysfsBlockDeviceStatsCollector, error) {
	// Resolve path → device major:minor → /sys/block/<device>/stat
	statPath, err := resolveBlockDeviceStatPath(path)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve block device stat path: %w", err)
	}

	pollCtx, cancel := context.WithCancel(ctx)

	collector := &SysfsBlockDeviceStatsCollector{
		path:     path,
		statPath: statPath,
		log:      log,
		nowFunc:  nowFunc,
		cancel:   cancel,
		done:     make(chan struct{}),
	}

	// Start poll loop in constructor
	go collector.pollLoop(pollCtx, pollInterval)

	return collector, nil
}

// resolveBlockDeviceStatPath resolves a filesystem path to its block device stat file
//
// Algorithm:
// 1. Stat the path to get device major:minor
// 2. Read symlink /sys/dev/block/major:minor to get device path
// 3. Ascend directory tree while:
//   - There's a "partition" file (indicates this is a partition)
//   - AND there's no "stat" file (we need to go to parent device)
//
// 4. Return the stat file path of the block device (not partition)
func resolveBlockDeviceStatPath(path string) (string, error) {
	// Get device major:minor from path
	var stat unix.Stat_t
	if err := unix.Stat(path, &stat); err != nil {
		return "", fmt.Errorf("failed to stat path: %w", err)
	}

	major := unix.Major(stat.Dev)
	minor := unix.Minor(stat.Dev)

	// Read symlink to get device path in sysfs
	devLink := fmt.Sprintf("/sys/dev/block/%d:%d", major, minor)
	devPath, err := os.Readlink(devLink)
	if err != nil {
		return "", fmt.Errorf("failed to read device symlink %s: %w", devLink, err)
	}

	// devPath is relative to /sys/dev/block, make it absolute
	devPath = filepath.Join("/sys/dev/block", devPath)
	devPath = filepath.Clean(devPath)

	// Ascend while we're at a partition and stat file doesn't exist
	for {
		statPath := filepath.Join(devPath, "stat")
		partitionPath := filepath.Join(devPath, "partition")

		// Check if stat file exists
		if _, err := os.Stat(statPath); err == nil {
			// stat file exists, we found the device
			return statPath, nil
		}

		// Check if this is a partition
		if _, err := os.Stat(partitionPath); os.IsNotExist(err) {
			// No partition file, this is not a partition, can't go further
			return "", fmt.Errorf("no stat file found and not a partition at %s", devPath)
		}

		// This is a partition and no stat file, ascend to parent
		devPath = filepath.Dir(devPath)
	}
}

func (c *SysfsBlockDeviceStatsCollector) pollLoop(ctx context.Context, interval time.Duration) {
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

func (c *SysfsBlockDeviceStatsCollector) collect() {
	// Check if a collection is already in progress
	if !c.collecting.CompareAndSwap(false, true) {
		// Already collecting, skip this poll to avoid spawning unbounded goroutines
		c.log.Warn().Msg("sysfs collection still in progress, skipping poll")

		// Still increment epoch to show we attempted collection
		c.mu.Lock()
		c.epoch++
		c.mu.Unlock()

		return
	}

	// Increment epoch BEFORE attempting sysfs read
	c.mu.Lock()
	c.epoch++
	epoch := c.epoch
	c.mu.Unlock()

	// Bounded timeout for sysfs read
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Perform sysfs read in a goroutine so we can timeout
	type result struct {
		stats *domain.BlockDeviceStats
		err   error
	}
	resultCh := make(chan result, 1)

	go func() {
		defer c.collecting.Store(false) // Clear flag when done

		stats, err := c.readSysfs()
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
		// Note: The goroutine with sysfs read is still blocked, but we move on
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
	// If err != nil (timeout or read error), Time is NOT updated → Epoch advances but Time frozen
}

func (c *SysfsBlockDeviceStatsCollector) readSysfs() (*domain.BlockDeviceStats, error) {
	// Read /sys/block/<device>/stat
	// Format: https://www.kernel.org/doc/Documentation/block/stat.txt
	//
	// Field order (space-separated):
	//  1 - reads completed successfully
	//  2 - reads merged
	//  3 - sectors read
	//  4 - time spent reading (ms)
	//  5 - writes completed
	//  6 - writes merged
	//  7 - sectors written
	//  8 - time spent writing (ms)
	//  9 - I/Os currently in progress
	// 10 - time spent doing I/Os (ms)
	// 11 - weighted time spent doing I/Os (ms)
	// 12 - discards completed successfully (optional, kernel 4.18+)
	// 13 - discards merged (optional, kernel 4.18+)
	// 14 - sectors discarded (optional, kernel 4.18+)
	// 15 - time spent discarding (ms) (optional, kernel 4.18+)
	// 16 - flush requests completed successfully (optional, kernel 5.5+)
	// 17 - time spent flushing (ms) (optional, kernel 5.5+)

	data, err := os.ReadFile(c.statPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read stat file: %w", err)
	}

	fields := strings.Fields(string(data))
	if len(fields) < 11 {
		return nil, fmt.Errorf("unexpected stat format: expected at least 11 fields, got %d", len(fields))
	}

	parseInt64 := func(s string) int64 {
		v, _ := strconv.ParseInt(s, 10, 64)
		return v
	}

	stats := &domain.BlockDeviceStatsLinux{
		Reads:          parseInt64(fields[0]),
		MergedReads:    parseInt64(fields[1]),
		SectorsRead:    parseInt64(fields[2]),
		ReadTicks:      parseInt64(fields[3]),
		Writes:         parseInt64(fields[4]),
		MergedWrites:   parseInt64(fields[5]),
		SectorsWritten: parseInt64(fields[6]),
		WriteTicks:     parseInt64(fields[7]),
		InFlight:       parseInt64(fields[8]),
		IOTicks:        parseInt64(fields[9]),
		TimeInQueue:    parseInt64(fields[10]),
	}

	// Optional fields (newer kernels)
	if len(fields) >= 15 {
		stats.Discards = parseInt64(fields[11])
		stats.MergedDiscards = parseInt64(fields[12])
		stats.SectorsDiscarded = parseInt64(fields[13])
		stats.DiscardTicks = parseInt64(fields[14])
	}

	if len(fields) >= 17 {
		stats.Flushes = parseInt64(fields[15])
		stats.FlushTicks = parseInt64(fields[16])
	}

	return &domain.BlockDeviceStats{
		Linux: stats,
	}, nil
}

// CollectVolumeStats implements VolumeStatsCollector interface
func (c *SysfsBlockDeviceStatsCollector) CollectVolumeStats() (domain.Sample[*domain.BlockDeviceStats], error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cachedSample, nil
}

func (c *SysfsBlockDeviceStatsCollector) Close() error {
	if c.cancel != nil {
		c.cancel()
		<-c.done
	}
	return nil
}
