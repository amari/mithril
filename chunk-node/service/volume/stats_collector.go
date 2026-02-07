package volume

import (
	"context"
	"time"

	"github.com/amari/mithril/chunk-node/domain"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
	"github.com/rs/zerolog"
)

// volumeStatsCollector is the internal interface for composing stats from multiple collectors
type volumeStatsCollector interface {
	CollectStats(ctx context.Context) (domain.VolumeStats, error)
	Close() error
}

// directoryVolumeStatsCollector composes independent adapter collectors
type directoryVolumeStatsCollector struct {
	volumeID domain.VolumeID

	spaceCollector portvolume.VolumeStatsCollector[*domain.SpaceUtilizationStats]
	blockCollector portvolume.VolumeStatsCollector[*domain.BlockDeviceStats]
}

// newDirectoryVolumeStatsCollector creates a stats collector that composes
// platform-specific adapter collectors running in parallel
func newDirectoryVolumeStatsCollector(
	ctx context.Context,
	volumeID domain.VolumeID,
	path string,
	pollInterval time.Duration,
	log *zerolog.Logger,
) (volumeStatsCollector, error) {
	// Create space utilization collector (statfs)
	spaceCollector, err := newSpaceUtilizationStatsCollector(ctx, path, pollInterval, log)
	if err != nil {
		return nil, err
	}

	// Create platform-specific block device collector (IOKit or sysfs)
	blockCollector, err := newBlockDeviceStatsCollector(ctx, path, pollInterval, log)
	if err != nil {
		// Block device stats are optional - might not be available
		log.Debug().Err(err).Msg("block device stats collector unavailable")
		blockCollector = nil
	}

	return &directoryVolumeStatsCollector{
		volumeID:       volumeID,
		spaceCollector: spaceCollector,
		blockCollector: blockCollector,
	}, nil
}

// CollectStats reads cached values from each collector (fast, no blocking syscalls)
func (c *directoryVolumeStatsCollector) CollectStats(ctx context.Context) (domain.VolumeStats, error) {
	spaceSample, _ := c.spaceCollector.CollectVolumeStats()

	stats := domain.VolumeStats{
		VolumeID:         c.volumeID,
		SpaceUtilization: spaceSample,
	}

	if c.blockCollector != nil {
		blockSample, _ := c.blockCollector.CollectVolumeStats()
		stats.BlockDevice = blockSample
	}

	return stats, nil
}

func (c *directoryVolumeStatsCollector) Close() error {
	var firstErr error

	if closer, ok := c.spaceCollector.(interface{ Close() error }); ok {
		if err := closer.Close(); err != nil {
			firstErr = err
		}
	}

	if c.blockCollector != nil {
		if closer, ok := c.blockCollector.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}

	return firstErr
}
