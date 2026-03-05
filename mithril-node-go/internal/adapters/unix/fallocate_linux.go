//go:build linux
// +build linux

package adaptersunix

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// Fallocate allocates disk space for the file referred to by fd and sets the
// file size to size bytes.
//
// It uses the Linux fallocate syscall with mode 0, which reserves space and
// grows the file as needed. On filesystems that support it (e.g., XFS, ext4),
// this ensures that space is actually reserved on disk.
//
// Returns an error if the syscall fails.
func Fallocate(fd int, mode uint32, off int64, len int64) error {
	err := ignoringEINTR(func() error {
		return unix.Fallocate(fd, 0, off, len)
	})

	if err != nil {
		return fmt.Errorf("fallocate: %w", err)
	}

	return nil
}
