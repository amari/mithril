package portvolume

import (
	"github.com/amari/mithril/chunk-node/domain"
)

// VolumeHealthChecker defines the interface for checking the health of a volume.
type VolumeHealthChecker interface {
	CheckVolumeHealth(v domain.VolumeID) *domain.VolumeHealth
}
