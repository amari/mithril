//go:build darwin
// +build darwin

package volume

import (
	darwin "github.com/amari/mithril/chunk-node/adapter/volume/labeler/darwin"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
)

// newDirectoryVolumeLabelCollector creates a platform-specific label collector for a directory volume.
func newDirectoryVolumeLabelCollector(path string) (portvolume.VolumeLabelCollector, error) {
	return darwin.NewIOKitVolumeLabelCollector(path)
}
