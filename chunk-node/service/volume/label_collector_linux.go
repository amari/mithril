//go:build linux
// +build linux

package volume

import (
	linux "github.com/amari/mithril/chunk-node/adapter/volume/labeler/linux"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
)

// newDirectoryVolumeLabelCollector creates a platform-specific label collector for a directory volume.
func newDirectoryVolumeLabelCollector(path string) (portvolume.VolumeLabelCollector, error) {
	return linux.NewSysfsVolumeLabelCollector(path)
}
