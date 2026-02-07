package unix

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// Ftruncate truncates or extends the file referred to by fd to the specified length.
func Ftruncate(fd int, length int64) error {
	err := ignoringEINTR(func() error {
		return unix.Ftruncate(fd, length)
	})
	if err != nil {
		return fmt.Errorf("ftruncate: %w", err)
	}

	return nil
}
