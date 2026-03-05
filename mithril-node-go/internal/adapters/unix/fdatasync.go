//go:build unix && !linux
// +build unix,!linux

package adaptersunix

// Fdatasync flushes file data to disk for the given file descriptor.
// On Darwin, it delegates to Fsync.
func Fdatasync(fd int) error {
	return Fsync(fd)
}
