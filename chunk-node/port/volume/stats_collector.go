package volume

import "github.com/amari/mithril/chunk-node/domain"

type VolumeStatsCollector[T any] interface {
	CollectVolumeStats() (domain.Sample[T], error)
	Close() error
}
