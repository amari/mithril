//go:build darwin
// +build darwin

package volumedirectorystatscollector

import (
	"fmt"

	samplerdarwin "github.com/amari/mithril/chunk-node/adapter/volume/sampler/darwin"
	samplerunix "github.com/amari/mithril/chunk-node/adapter/volume/sampler/unix"
	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/port/volume"
)

type darwinDirectoryVolumeStatsCollector struct {
	volumeID domain.VolumeID

	blockDevice      *samplerdarwin.IOKitBlockDeviceStatSampler
	spaceUtilization *samplerunix.StatfsSpaceUtilizationStatSampler
}

var _ volume.VolumeStatsCollector = (*darwinDirectoryVolumeStatsCollector)(nil)

func newDirectoryVolumeStatsCollector(opts DirectoryVolumeStatsCollectorOptions) (*darwinDirectoryVolumeStatsCollector, error) {
	blockDevice, err := samplerdarwin.NewIOKitBlockDeviceStatSamplerWithPath(
		opts.Path,
		samplerdarwin.IOKitBlockDeviceStatSamplerOptions{
			PollInterval: opts.PollInterval,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create block device sampler: %w", err)
	}

	spaceUtilization, err := samplerunix.NewStatfsSpaceUtilizationStatSampler(
		opts.Path,
		samplerunix.StatfsSpaceUtilizationStatSamplerOptions{
			PollInterval: opts.PollInterval,
		},
	)
	if err != nil {
		blockDevice.Close()
		return nil, fmt.Errorf("failed to create space utilization sampler: %w", err)
	}

	return &darwinDirectoryVolumeStatsCollector{
		volumeID:         opts.VolumeID,
		blockDevice:      blockDevice,
		spaceUtilization: spaceUtilization,
	}, nil
}

func (c *darwinDirectoryVolumeStatsCollector) CollectVolumeStats() *domain.VolumeStats {
	return &domain.VolumeStats{
		VolumeID:         c.volumeID,
		BlockDevice:      c.blockDevice.GetSample(),
		SpaceUtilization: c.spaceUtilization.GetSample(),
	}
}

func (c *darwinDirectoryVolumeStatsCollector) Close() error {
	blockDeviceErr := c.blockDevice.Close()
	spaceUtilizationErr := c.spaceUtilization.Close()

	if blockDeviceErr != nil {
		return blockDeviceErr
	}
	return spaceUtilizationErr
}
