package portvolume

import "github.com/amari/mithril/chunk-node/domain"

// VolumePicker defines the interface for picking volume IDs.
type VolumePicker interface {
	// PickVolumeID picks a volume ID based on the provided options.
	PickVolumeID(opts PickVolumeIDOptions) (domain.VolumeID, error)

	// SetVolumeIDs sets the available volume IDs to pick from.
	SetVolumeIDs([]domain.VolumeID)

	// UpdateVolumeID updates the information of a specific volume ID.
	UpdateVolumeID(v domain.VolumeID)
}

// PickVolumeIDOptions contains options for picking a volume ID.
type PickVolumeIDOptions struct {
	// ProbeFunc is the probing function to use when picking a volume ID. If nil, LinearProbe is used.
	ProbeFunc func(total, start, attempt int) int
	// MaxProbeAttempts is the maximum number of probe attempts to make when picking a volume ID. If zero, all volume IDs are probed.
	MaxProbeAttempts int
	// Filter is the filter to apply when picking a volume ID. If nil, no filtering is applied.
	Filter VolumeIDPickFilter
}

// VolumeIDPickFilter defines the interface for filtering volume IDs.
type VolumeIDPickFilter interface {
	FilterVolumeIDPick(v domain.VolumeID) bool
}
