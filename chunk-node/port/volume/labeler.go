package volume

import (
	"context"

	"github.com/amari/mithril/chunk-node/domain"
)

// VolumeLabeler provides labels for a volume.
type VolumeLabeler interface {
	VolumeLabels(ctx context.Context, v domain.VolumeID) (map[string]string, error)
}
