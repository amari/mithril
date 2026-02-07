//go:build darwin
// +build darwin

package unix

import (
	"errors"
	"fmt"

	"golang.org/x/sys/unix"
)

// Clonefile creates a copy-on-write clone of the file at src to dst.
func Clonefile(src string, dst string, flags int) error {
	err := ignoringEINTR(func() error {
		return unix.Clonefile(src, dst, 0)
	})
	if err != nil {
		return fmt.Errorf("clonefile: %w", err)
	}

	return ignoringEINTR(func() error {
		return unix.Clonefile(src, dst, 0)
	})
}

// Clonefileat creates a copy-on-write clone of the file at src to dst using directory file descriptors.
func Clonefileat(srcDirfd int, src string, dstDirfd int, dst string, flags int) error {
	for {
		err := unix.Clonefileat(srcDirfd, src, dstDirfd, dst, 0)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return fmt.Errorf("clonefileat: %w", err)
		}
		return nil
	}
}

// Fclonefileat creates a copy-on-write clone of the file referred to by srcFd to dst using a directory file descriptor.
func Fclonefileat(srcFd int, dstDirfd int, dst string, flags int) error {
	for {
		err := unix.Fclonefileat(srcFd, dstDirfd, dst, 0)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return fmt.Errorf("clonefileat: %w", err)
		}
		return nil
	}
}
