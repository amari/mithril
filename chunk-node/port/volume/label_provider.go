package portvolume

import (
	"context"
)

// VolumeLabelProvider provides labels for a single volume.
type VolumeLabelProvider interface {
	// GetVolumeLabels returns the labels associated with this volume.
	GetVolumeLabels() map[string]string

	// Watch returns a channel that signals when the volume's labels have changed.
	Watch(watchCtx context.Context) <-chan struct{}
}

// VolumeLabelToIDsIndex maps label key/value pairs to volume IDs.
type VolumeLabelToIDsIndex interface {
	GetVolumeIDsByLabel(key, value string) []ID
}

// VolumeLabelToIDSetIndex maps label key/value pairs to volume ID sets.
type VolumeLabelToIDSetIndex interface {
	GetVolumeIDSetByLabel(key, value string) *IDSet
	GetAllVolumeIDSets() map[string]map[string]*IDSet
}
