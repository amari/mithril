//go:build linux
// +build linux

package volumedirectorystatscollector

import (
	"fmt"

	samplerlinux "github.com/amari/mithril/chunk-node/adapter/volume/sampler/linux"
	samplerunix "github.com/amari/mithril/chunk-node/adapter/volume/sampler/unix"
	"github.com/amari/mithril/chunk-node/domain"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
)

type linuxDirectoryVolumeStatsProvider struct {
	volumeID domain.VolumeID

	blockDevice      *samplerlinux.SysfsBlockDeviceStatSampler
	spaceUtilization *samplerunix.StatfsSpaceUtilizationStatSampler
}

var _ portvolume.VolumeStatsProvider = (*linuxDirectoryVolumeStatsProvider)(nil)

func newDirectoryVolumeStatsProvider(opts DirectoryVolumeStatsProviderOptions) (*linuxDirectoryVolumeStatsProvider, error) {
	blockDevice, err := samplerlinux.NewSysfsBlockDeviceStatSamplerWithPath(
		opts.Path,
		samplerlinux.SysfsBlockDeviceStatSamplerOptions{
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

	return &linuxDirectoryVolumeStatsProvider{
		volumeID:         opts.VolumeID,
		blockDevice:      blockDevice,
		spaceUtilization: spaceUtilization,
	}, nil
}

func (c *linuxDirectoryVolumeStatsProvider) GetVolumeStats() *domain.VolumeStats {
	return &domain.VolumeStats{
		VolumeID:         c.volumeID,
		BlockDevice:      c.blockDevice.GetSample(),
		SpaceUtilization: c.spaceUtilization.GetSample(),
	}
}

func (c *linuxDirectoryVolumeStatsProvider) Stop() error {
	blockDeviceErr := c.blockDevice.Close()
	spaceUtilizationErr := c.spaceUtilization.Close()

	if blockDeviceErr != nil {
		return blockDeviceErr
	}
	return spaceUtilizationErr
}
