package portvolume

import "github.com/amari/mithril/chunk-node/domain"

type VolumeStatsCollector interface {
	CollectVolumeStats() *domain.VolumeStats
	Close() error
}
