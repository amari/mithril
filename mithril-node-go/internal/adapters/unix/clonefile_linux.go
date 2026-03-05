//go:build linux
// +build linux

package adaptersunix

import (
	"errors"
	"fmt"

	"golang.org/x/sys/unix"
)

// Clonefile creates a copy-on-write clone of the file at src to dst.
func Clonefile(src string, dst string, flags int) error {
	var srcFd int
	for {
		var err error
		srcFd, err = unix.Open(src, unix.O_RDONLY, 0666)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return fmt.Errorf("openat: %w", err)
		}
		defer unix.Close(srcFd)

		break
	}

	var dstFd int
	for {
		var err error

		dstFd, err = unix.Open(dst, unix.O_CREAT|unix.O_WRONLY|unix.O_TRUNC, 0666)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return fmt.Errorf("openat: %w", err)
		}
		defer unix.Close(dstFd)

		break
	}

	for {
		err := unix.IoctlFileClone(dstFd, srcFd)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return fmt.Errorf("ioctl: %w", err)
		}

		break
	}

	return nil
}

// Clonefileat creates a copy-on-write clone of the file at src to dst using directory file descriptors.
func Clonefileat(srcDirfd int, src string, dstDirfd int, dst string, flags int) error {
	var srcFd int
	for {
		var err error

		srcFd, err = unix.Openat(srcDirfd, src, unix.O_RDONLY, 0666)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return fmt.Errorf("openat: %w", err)
		}
		defer unix.Close(srcFd)

		break
	}

	return Fclonefileat(srcFd, dstDirfd, dst, flags)
}

// Fclonefileat creates a copy-on-write clone of the file referred to by srcFd to dst using a directory file descriptor.
func Fclonefileat(srcFd int, dstDirfd int, dst string, flags int) error {
	var dstFd int
	for {
		var err error

		dstFd, err = unix.Openat(dstDirfd, dst, unix.O_CREAT|unix.O_WRONLY|unix.O_TRUNC, 0666)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return fmt.Errorf("openat: %w", err)
		}
		defer unix.Close(dstFd)

		break
	}

	for {
		err := unix.IoctlFileClone(dstFd, srcFd)

		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return fmt.Errorf("ioctl: %w", err)
		}

		break
	}

	return nil
}
