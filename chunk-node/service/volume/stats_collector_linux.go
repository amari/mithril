//go:build linux

package volume

import (
	"context"
	"time"

	"github.com/amari/mithril/chunk-node/adapter/volume/stats/linux"
	"github.com/amari/mithril/chunk-node/domain"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
	"github.com/rs/zerolog"
)

// newBlockDeviceStatsCollector creates a sysfs-based block device stats collector
func newBlockDeviceStatsCollector(
	ctx context.Context,
	path string,
	pollInterval time.Duration,
	log *zerolog.Logger,
) (portvolume.VolumeStatsCollector[*domain.BlockDeviceStats], error) {
	return linux.NewSysfsBlockDeviceStatsCollector(ctx, path, pollInterval, log, time.Now)
}
