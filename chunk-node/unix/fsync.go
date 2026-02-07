package unix

import (
	"errors"
)

// ErrFsync is a sentinel error indicating that an fsync operation failed.
// It can be used with errors.Is to detect fsync-related failures.
var ErrFsync = errors.New("fsync failed")

// FsyncError wraps a causal error and marks it as an fsync failure.
// It allows errors.Is to match ErrFsync while preserving the original error.
type FsyncError struct {
	cause error
}

var _ error = (*FsyncError)(nil)

func NewFsyncError(cause error) error {
	return &FsyncError{cause: cause}
}

func (e *FsyncError) Error() string {
	if e == nil || e.cause == nil {
		return "fsync failed"
	}

	return "fsync failed: " + e.cause.Error()
}

func (e *FsyncError) Unwrap() []error {
	if e == nil || e.cause == nil {
		return []error{ErrFsync}
	}

	return []error{ErrFsync, e.cause}
}
