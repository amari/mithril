package errors

import "errors"

// Volume Lifecycle Errors
var (
	// ErrVolumeNotFound indicates a volume with the given ID doesn't exist.
	ErrVolumeNotFound = errors.New("volume not found")

	// ErrVolumeAlreadyRegistered indicates a volume with this ID is already registered.
	ErrVolumeAlreadyRegistered = errors.New("volume already registered")
)

// Volume Format Errors
var (
	// ErrVolumeNotFormatted indicates the directory/device is not formatted as a volume.
	// The caller should format it before opening.
	ErrVolumeNotFormatted = errors.New("volume not formatted")

	// ErrVolumeAlreadyFormatted indicates the directory/device is already formatted.
	// Format operations are idempotent checks - reformatting would destroy data.
	ErrVolumeAlreadyFormatted = errors.New("volume already formatted")

	// ErrVolumeCorrupted indicates the volume format is malformed or corrupted.
	// The volume may be unreadable and require manual intervention.
	ErrVolumeCorrupted = errors.New("volume corrupted")

	// ErrVolumeWrongNode indicates the volume was formatted for a different node.
	// This is a safety check to prevent cross-node volume access.
	ErrVolumeWrongNode = errors.New("volume wrong node")
)

// Volume Capacity & Health Errors
var (
	// ErrVolumeNoSpace indicates the volume has insufficient free space.
	// The operation requires more space than is available.
	ErrVolumeNoSpace = errors.New("volume no space")

	// ErrVolumeDegraded indicates the volume is in a degraded state.
	// Operations may still succeed but with reduced reliability/performance.
	// Read operations typically allowed, writes should be avoided.
	ErrVolumeDegraded = errors.New("volume degraded")

	// ErrVolumeFailed indicates the volume has permanently failed.
	// Most operations will fail. Only Stat operations should proceed.
	ErrVolumeFailed = errors.New("volume failed")

	// ErrVolumeReadOnly indicates the volume is mounted read-only.
	// Write operations will fail.
	ErrVolumeReadOnly = errors.New("volume read only")
)

// Volume Selection Errors
var (
	// ErrNoVolumesAvailable indicates no volumes passed the selection criteria.
	// This could mean: no volumes registered, all volumes full, all volumes unhealthy, etc.
	ErrNoVolumesAvailable = errors.New("no volumes available")

	// ErrVolumeIDExhausted indicates all 65535 volume IDs have been allocated.
	// This is a fatal condition requiring operator intervention.
	ErrVolumeIDExhausted = errors.New("volume id exhausted")
)

type Volume struct {
	State VolumeState
}

type VolumeState int

const (
	VolumeStateUnknown VolumeState = iota
	VolumeStateOk
	VolumeStateDegraded
	VolumeStateFailed
)
