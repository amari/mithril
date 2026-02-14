package volumedirectorystatscollector

import (
	"time"

	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/port/volume"
)

type DirectoryVolumeStatsCollectorOptions struct {
	VolumeID domain.VolumeID
	Path     string

	PollInterval time.Duration
}

func NewDirectoryVolumeStatsCollector(opts DirectoryVolumeStatsCollectorOptions) (volume.VolumeStatsCollector, error) {
	return newDirectoryVolumeStatsCollector(opts)
}
