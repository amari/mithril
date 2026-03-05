//go:build linux
// +build linux

package adapterssysfs

import "errors"

// Error sentinels for Linux sysfs operations.
var (
	// ErrVirtualFilesystem indicates the path resides on a virtual filesystem
	// that doesn't correspond to a real block device. This includes OverlayFS,
	// tmpfs, devfs, sysfs, procfs, and similar pseudo-filesystems.
	//
	// Virtual filesystems have a major device number of 0, which means there's
	// no backing block device to collect stats from or label.
	ErrVirtualFilesystem = errors.New("virtual filesystem has no backing block device")

	// ErrNoBlockDevice indicates the sysfs device path could not be resolved.
	// This may occur when /sys/dev/block/<major>:<minor> doesn't exist or the
	// symlink cannot be read.
	ErrNoBlockDevice = errors.New("no block device found in sysfs")

	// ErrNoStatFile indicates no stat file was found for the block device.
	// This typically means the device doesn't expose statistics via sysfs.
	ErrNoStatFile = errors.New("no stat file found for block device")

	// ErrInvalidStatFormat indicates the stat file content could not be parsed.
	// The file should contain at least 11 space-separated integer fields as
	// documented in https://www.kernel.org/doc/Documentation/block/stat.txt
	ErrInvalidStatFormat = errors.New("invalid block device stat format")
)
