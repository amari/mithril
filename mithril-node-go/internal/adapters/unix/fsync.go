//go:build unix && !darwin
// +build unix,!darwin

package adaptersunix

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

			return err
		}

		return nil
	}
}
