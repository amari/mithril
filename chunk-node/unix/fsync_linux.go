//go:build linux
// +build linux

package unix

import (
	"errors"

	"golang.org/x/sys/unix"
)

// Fsync performs a full fsync on the given file descriptor.
func Fsync(fd int) error {
	for {
		err := unix.Fsync(fd)
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
func Fdatasync(fd int) error {
	for {
		err := unix.Fdatasync(fd)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}

			return NewFsyncError(err)
		}

		return nil
	}
}
