//go:build darwin
// +build darwin

package volumedirectorystatscollector

import (
	"fmt"

	samplerdarwin "github.com/amari/mithril/chunk-node/adapter/volume/sampler/darwin"
	samplerunix "github.com/amari/mithril/chunk-node/adapter/volume/sampler/unix"
	"github.com/amari/mithril/chunk-node/domain"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
)

type darwinDirectoryVolumeStatsProvider struct {
	volumeID domain.VolumeID

	blockDevice      *samplerdarwin.IOKitBlockDeviceStatSampler
	spaceUtilization *samplerunix.StatfsSpaceUtilizationStatSampler
}

var _ portvolume.VolumeStatsProvider = (*darwinDirectoryVolumeStatsProvider)(nil)

func newDirectoryVolumeStatsProvider(opts DirectoryVolumeStatsProviderOptions) (*darwinDirectoryVolumeStatsProvider, error) {
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

	return &darwinDirectoryVolumeStatsProvider{
		volumeID:         opts.VolumeID,
		blockDevice:      blockDevice,
		spaceUtilization: spaceUtilization,
	}, nil
}

func (c *darwinDirectoryVolumeStatsProvider) GetVolumeStats() *domain.VolumeStats {
	return &domain.VolumeStats{
		VolumeID:         c.volumeID,
		BlockDevice:      c.blockDevice.GetSample(),
		SpaceUtilization: c.spaceUtilization.GetSample(),
	}
}

func (c *darwinDirectoryVolumeStatsProvider) Stop() error {
	blockDeviceErr := c.blockDevice.Close()
	spaceUtilizationErr := c.spaceUtilization.Close()

	if blockDeviceErr != nil {
		return blockDeviceErr
	}

	return spaceUtilizationErr
}
