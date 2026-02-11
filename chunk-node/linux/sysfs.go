//go:build linux
// +build linux

// Package linux provides sysfs utilities for interacting with Linux block devices.
//
// This package provides shared functionality for:
//   - Resolving filesystem paths to their underlying block devices via sysfs
//   - Detecting virtual filesystems (major device number 0)
//   - Navigating from partitions to parent block devices
//
// # Sysfs Block Device Layout
//
// Linux exposes block device information through sysfs at /sys/dev/block/<major>:<minor>.
// This symlink points to the actual device directory under /sys/devices or /sys/block.
//
// For partitions, the directory contains a "partition" file indicating partition number.
// The parent directory represents the whole block device.
//
// # Virtual Filesystems
//
// Filesystems with major device number 0 are virtual (OverlayFS, tmpfs, devfs, sysfs, etc.)
// and do not correspond to real block devices. Operations requiring block device information
// will return ErrVirtualFilesystem for these paths.
package linux

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

// BlockDevice represents a resolved block device from sysfs.
type BlockDevice struct {
	// SysfsPath is the absolute path to the block device directory in sysfs.
	// Example: /sys/devices/pci0000:00/0000:00:1f.2/ata1/host0/target0:0:0/0:0:0:0/block/sda
	SysfsPath string

	// Major is the major device number.
	Major uint32

	// Minor is the minor device number.
	Minor uint32
}

// ResolveBlockDevice resolves a filesystem path to its underlying block device.
//
// The function:
//  1. Stats the path to get device major:minor
//  2. Validates that major != 0 (real block device, not virtual filesystem)
//  3. Reads symlink /sys/dev/block/major:minor to get sysfs device path
//  4. Ascends directory tree while at a partition to get to the parent block device
//
// Returns ErrVirtualFilesystem if the path is on a virtual filesystem (major == 0).
// Returns ErrNoBlockDevice if the sysfs symlink cannot be resolved.
func ResolveBlockDevice(path string) (*BlockDevice, error) {
	var stat unix.Stat_t
	if err := unix.Stat(path, &stat); err != nil {
		return nil, fmt.Errorf("stat %s: %w", path, err)
	}

	major := unix.Major(stat.Dev)
	minor := unix.Minor(stat.Dev)

	// Major device number 0 indicates a virtual filesystem
	// (OverlayFS, tmpfs, devfs, sysfs, proc, etc.)
	if major == 0 {
		return nil, fmt.Errorf("%w: major device number is 0 (path: %s)", ErrVirtualFilesystem, path)
	}

	// Read symlink to get device path in sysfs
	devLink := fmt.Sprintf("/sys/dev/block/%d:%d", major, minor)
	devPath, err := os.Readlink(devLink)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to read symlink %s: %v", ErrNoBlockDevice, devLink, err)
	}

	// devPath is relative to /sys/dev/block, make it absolute
	devPath = filepath.Join("/sys/dev/block", devPath)
	devPath = filepath.Clean(devPath)

	// Ascend while we're at a partition to get to the parent block device
	for {
		partitionPath := filepath.Join(devPath, "partition")

		// Check if this is a partition
		if _, err := os.Stat(partitionPath); os.IsNotExist(err) {
			// No partition file, this is the actual block device
			break
		}

		// This is a partition, ascend to parent
		devPath = filepath.Dir(devPath)
	}

	return &BlockDevice{
		SysfsPath: devPath,
		Major:     major,
		Minor:     minor,
	}, nil
}

// StatPath returns the path to the block device stat file.
// The stat file contains I/O statistics as documented in:
// https://www.kernel.org/doc/Documentation/block/stat.txt
func (d *BlockDevice) StatPath() string {
	return filepath.Join(d.SysfsPath, "stat")
}

// ReadFile reads a file relative to the block device's sysfs path.
// Returns the file contents with leading/trailing whitespace trimmed.
func (d *BlockDevice) ReadFile(relPath string) (string, error) {
	fullPath := filepath.Join(d.SysfsPath, relPath)
	data, err := os.ReadFile(fullPath)
	if err != nil {
		return "", err
	}
	// Trim whitespace - sysfs files often have trailing newlines
	return strings.TrimSpace(string(data)), nil
}
