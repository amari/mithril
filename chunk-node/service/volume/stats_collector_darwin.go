//go:build darwin

package volume

import (
	"context"
	"time"

	"github.com/amari/mithril/chunk-node/adapter/volume/stats/darwin"
	"github.com/amari/mithril/chunk-node/domain"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
	"github.com/rs/zerolog"
)

// newBlockDeviceStatsCollector creates an IOKit-based block device stats collector
func newBlockDeviceStatsCollector(
	ctx context.Context,
	path string,
	pollInterval time.Duration,
	log *zerolog.Logger,
) (portvolume.VolumeStatsCollector[*domain.BlockDeviceStats], error) {
	return darwin.NewIOKitBlockDeviceStatsCollector(ctx, path, pollInterval, log)
}
