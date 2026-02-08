// service/volume/error_classifier.go
package volume

import (
	"context"
	"errors"
	"os"
	"syscall"

	"github.com/amari/mithril/chunk-node/unix"
	"github.com/amari/mithril/chunk-node/volumeerrors"
)

// VolumeErrorClass categorizes volume errors for health state transitions.
type VolumeErrorClass int

const (
	VolumeErrorClassTransient VolumeErrorClass = iota // No state change
	VolumeErrorClassDegrading                         // Ok → Degraded (recoverable)
	VolumeErrorClassFatal                             // → Failed (permanent)
)

// classifyVolumeError maps system errors to volume health impact.
//
// Error classification:
// - ENOSPC: Degrading (space can be freed)
// - EIO: Fatal (hardware/filesystem corruption)
// - Fsync errors: Fatal (data integrity compromise)
// - Timeout errors: Degrading (transient overload)
// - Unknown errors: Transient (no state change)
func classifyVolumeError(err error) VolumeErrorClass {
	if err == nil {
		return VolumeErrorClassTransient
	}

	// Fsync errors - data integrity compromise, always fatal
	if errors.Is(err, unix.ErrFsync) {
		return VolumeErrorClassFatal
	}

	// ENOSPC - no space, degrading but recoverable
	if errors.Is(err, syscall.ENOSPC) || errors.Is(err, volumeerrors.ErrNoSpace) {
		return VolumeErrorClassDegrading
	}

	// EIO - I/O error, likely hardware/filesystem corruption
	if errors.Is(err, syscall.EIO) {
		return VolumeErrorClassFatal
	}

	// Timeout errors - system overload or slow I/O
	if isTimeoutError(err) {
		return VolumeErrorClassDegrading
	}

	// Unknown error - treat as transient
	return VolumeErrorClassTransient
}

// isTimeoutError checks if error is a timeout.
// Timeouts indicate system overload or slow I/O, which are typically transient
// but can indicate a degrading volume if they happen frequently.
func isTimeoutError(err error) bool {
	// Check for context deadline exceeded
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	// Check for os deadline exceeded
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}

	// Check if error has Timeout() method
	type timeout interface {
		Timeout() bool
	}
	if te, ok := err.(timeout); ok {
		return te.Timeout()
	}

	return false
}
