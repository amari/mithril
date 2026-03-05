//go:build linux
// +build linux

package adaptersunix

import (
	"errors"

	"golang.org/x/sys/unix"
)

// Fdatasync flushes file data to disk for the given file descriptor.
func Fdatasync(fd int) error {
	for {
		err := unix.Fdatasync(fd)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}

			return err
		}

		return nil
	}
}
