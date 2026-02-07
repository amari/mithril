package domain

type VolumeID uint16

type DirectoryVolume struct {
	VolumeID VolumeID
	Path     string
}

type VolumeState int

const (
	VolumeStateUnknown VolumeState = iota
	VolumeStateOK
	VolumeStateDegraded
	VolumeStateFailed
)

func (vs VolumeState) String() string {
	switch vs {
	case VolumeStateOK:
		return "OK"
	case VolumeStateDegraded:
		return "Degraded"
	case VolumeStateFailed:
		return "Failed"
	default:
		return "Unknown"
	}
}

type VolumeHealth struct {
	State VolumeState
}
