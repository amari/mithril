package volume

import (
	"context"
	"time"

	"github.com/amari/mithril/chunk-node/adapter/volume/stats/unix"
	"github.com/amari/mithril/chunk-node/domain"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
	"github.com/rs/zerolog"
)

// newSpaceUtilizationStatsCollector creates a statfs-based space utilization collector
func newSpaceUtilizationStatsCollector(
	ctx context.Context,
	path string,
	pollInterval time.Duration,
	log *zerolog.Logger,
) (portvolume.VolumeStatsCollector[*domain.SpaceUtilizationStats], error) {
	return unix.NewStatfsSpaceUtilizationStatsCollector(ctx, path, pollInterval, log)
}
