//go:build linux
// +build linux

// Package linux provides sysfs utilities for interacting with Linux block devices.
//
// This package wraps Linux sysfs APIs to enable Go code to:
//   - Resolve filesystem paths to their underlying block devices
//   - Navigate the sysfs block device hierarchy
//   - Distinguish partitions from whole block devices
//   - Detect virtual filesystems (tmpfs, overlayfs, etc.)
//
// # Block Device Resolution
//
// Linux filesystems are backed by block devices identified by major:minor numbers.
// The major number identifies the device driver, while the minor number identifies
// the specific device instance. A major number of 0 indicates a virtual filesystem
// that is not backed by a real block device.
//
// The sysfs virtual filesystem exposes block device information at:
//
//	/sys/dev/block/<major>:<minor> → symlink to device directory
//	/sys/block/<device>/           → whole block device info
//	/sys/block/<device>/<partition>/ → partition info
//
// # Partition vs Block Device
//
// Partitions in sysfs contain a "partition" file with their partition number.
// When resolving a filesystem path to its block device, we traverse up the
// sysfs hierarchy from partition to parent block device as needed.
//
// # Virtual Filesystems
//
// Filesystems with a major device number of 0 are virtual and don't correspond
// to real block devices. Examples include:
//   - tmpfs (in-memory filesystem)
//   - overlayfs (union mount filesystem)
//   - devfs/devtmpfs (device filesystem)
//   - sysfs (kernel object filesystem)
//   - proc (process information filesystem)
//
// Operations that require a real block device (stats collection, label reading)
// will return ErrVirtualFilesystem for these paths.
package adapterssysfs
