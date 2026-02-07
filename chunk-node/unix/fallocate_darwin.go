//go:build darwin
// +build darwin

package unix

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// Fallocate allocates disk space for the file referred to by fd and sets the
// file size to size bytes.
//
// It first attempts to allocate a contiguous block of space using
// F_PREALLOCATE with F_ALLOCATECONTIG and F_ALLOCATEALL. If that fails (e.g., due to
// fragmentation), it falls back to non-contiguous allocation using
// F_ALLOCATEALL.
//
// Once space is allocated, it calls ftruncate to extend the file size to match.
//
// Returns an error if allocation or truncation fails.
func Fallocate(fd int, mode uint32, off int64, len int64) error {
	// Fix for macOS F_PREALLOCATE semantics
	len = off + len
	off = 0

	// Try to allocate contiguous space first
	err := ignoringEINTR(func() error {
		return unix.FcntlFstore(uintptr(fd), unix.F_PREALLOCATE, &unix.Fstore_t{
			Flags:   unix.F_ALLOCATECONTIG | unix.F_ALLOCATEALL,
			Posmode: unix.F_PEOFPOSMODE,
			Offset:  off,
			Length:  len,
		})
	})
	if err != nil {
		return fmt.Errorf("fcntl F_PREALLOCATE: %w", err)
	}

	// If contiguous allocation failed, try non-contiguous
	err = ignoringEINTR(func() error {
		return unix.FcntlFstore(uintptr(fd), unix.F_PREALLOCATE, &unix.Fstore_t{
			Flags:   unix.F_ALLOCATEALL,
			Posmode: unix.F_PEOFPOSMODE,
			Offset:  off,
			Length:  len,
		})
	})
	if err != nil {
		return fmt.Errorf("fcntl F_PREALLOCATE: %w", err)
	}

	// Truncate the file to the desired length
	return Ftruncate(fd, off+len)
}
