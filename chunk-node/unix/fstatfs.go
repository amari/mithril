package unix

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// Fstatfs retrieves filesystem statistics for the given file descriptor.
func Fstatfs(fd int) (*unix.Statfs_t, error) {
	var stat unix.Statfs_t

	err := ignoringEINTR(func() error {
		return unix.Fstatfs(fd, &stat)
	})
	if err != nil {
		return nil, fmt.Errorf("statfs: %w", err)
	}

	return &stat, nil
}
