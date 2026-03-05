package adaptersunix

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// Statfs retrieves filesystem statistics for the given path.
func Statfs(path string) (*unix.Statfs_t, error) {
	var stat unix.Statfs_t

	err := ignoringEINTR(func() error {
		return unix.Statfs(path, &stat)
	})
	if err != nil {
		return nil, fmt.Errorf("statfs: %w", err)
	}

	return &stat, nil
}
