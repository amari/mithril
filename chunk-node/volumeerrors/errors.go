package volumeerrors

import "errors"

var (
	// ErrNotFound indicates a volume with the given ID doesn't exist.
	ErrNotFound = errors.New("volume not found")
	// ErrAlreadyRegistered indicates a volume with this ID is already registered.
	ErrAlreadyRegistered = errors.New("volume already registered")
	// ErrNotFormatted indicates the directory/device is not formatted as a volume.
	// The caller should format it before opening.
	ErrNotFormatted = errors.New("volume not formatted")
	// ErrAlreadyFormatted indicates the directory/device is already formatted.
	// Format operations are idempotent checks - reformatting would destroy data.
	ErrAlreadyFormatted = errors.New("volume already formatted")
	// ErrCorrupted indicates the volume format is malformed or corrupted.
	// The volume may be unreadable and require manual intervention.
	ErrCorrupted = errors.New("volume corrupted")
	// ErrWrongNode indicates the volume was formatted for a different node.
	// This is a safety check to prevent cross-node volume access.
	ErrWrongNode = errors.New("volume wrong node")
	// ErrNoSpace indicates the volume has insufficient free space.
	// The operation requires more space than is available.
	ErrNoSpace = errors.New("volume no space")
	// ErrDegraded indicates the volume is in a degraded state.
	// Operations may still succeed but with reduced reliability/performance.
	// Read operations typically allowed, writes should be avoided.
	ErrDegraded = errors.New("volume degraded")
	// ErrFailed indicates the volume has permanently failed.
	// Most operations will fail. Only Stat operations should proceed.
	ErrFailed = errors.New("volume failed")
	// ErrReadOnly indicates the volume is mounted read-only.
	// Write operations will fail.
	ErrReadOnly = errors.New("volume read only")
	// ErrStatsUnavailable indicates the volume statistics are unavailable.
	ErrStatsUnavailable = errors.New("volume stats unavailable")
	// ErrNoVolumesAvailable indicates no volumes passed the selection criteria.
	// This could mean: no volumes registered, all volumes full, all volumes unhealthy, etc.
	ErrNoVolumesAvailable = errors.New("no volumes available")
	// ErrIDExhausted indicates all 65535 volume IDs have been allocated.
	// This is a fatal condition requiring operator intervention.
	ErrIDExhausted = errors.New("volume id exhausted")
)
