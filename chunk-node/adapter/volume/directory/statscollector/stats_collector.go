package volumedirectorystatscollector

import (
	"time"

	"github.com/amari/mithril/chunk-node/domain"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
)

type DirectoryVolumeStatsProviderOptions struct {
	VolumeID domain.VolumeID
	Path     string

	PollInterval time.Duration
}

func NewDirectoryVolumeStatsProvider(opts DirectoryVolumeStatsProviderOptions) (portvolume.VolumeStatsProvider, error) {
	return newDirectoryVolumeStatsProvider(opts)
}
