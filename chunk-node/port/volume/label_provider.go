package volume

import (
	"context"

	"github.com/amari/mithril/chunk-node/domain"
)

// / VolumeLabelProvider provides labels for a given volume.
type VolumeLabelProvider interface {
	// VolumeLabels returns a map of labels for the given volume.
	VolumeLabels(ctx context.Context, volumeID domain.VolumeID) (map[string]string, error)
}
