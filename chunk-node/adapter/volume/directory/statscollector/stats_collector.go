package volumedirectorystatscollector

import (
	"time"

	"github.com/amari/mithril/chunk-node/domain"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
)

type DirectoryVolumeStatsCollectorOptions struct {
	VolumeID domain.VolumeID
	Path     string

	PollInterval time.Duration
}

func NewDirectoryVolumeStatsCollector(opts DirectoryVolumeStatsCollectorOptions) (portvolume.VolumeStatsCollector, error) {
	return newDirectoryVolumeStatsCollector(opts)
}
