//go:build darwin
// +build darwin

package unix

import (
	"errors"

	"golang.org/x/sys/unix"
)

// Fsync performs a full fsync on the given file descriptor.
// On Darwin, this uses F_FULLFSYNC for extra data integrity.
func Fsync(fd int) error {
	for {
		_, err := unix.FcntlInt(uintptr(fd), unix.F_FULLFSYNC, 0)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}

			return NewFsyncError(err)
		}

		return nil
	}
}

// Fdatasync flushes file data to disk for the given file descriptor.
// On Darwin, it delegates to Fsync.
func Fdatasync(fd int) error {
	return Fsync(fd)
}
