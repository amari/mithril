//go:build linux
// +build linux

package linux

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	portvolume "github.com/amari/mithril/chunk-node/port/volume"
	"golang.org/x/sys/unix"
)

// Transport label key constants.
const (
	LabelNVMe   = "nvme"
	LabelUSB    = "usb"
	LabelVirtIO = "virtio"
	LabelSATA   = "sata"
	LabelPATA   = "pata"
	LabelSAS    = "sas"
	LabelSCSI   = "scsi"
	LabelISCSI  = "iscsi"
)

// Other label key constants.
const (
	LabelModel  = "model"
	LabelVendor = "vendor"
	LabelSSD    = "ssd"
	LabelHDD    = "hdd"
)

// SysfsVolumeLabelCollector collects volume labels by reading sysfs.
type SysfsVolumeLabelCollector struct {
	path       string
	sysDevPath string // /sys/dev/block/<major>:<minor> resolved to actual device path
}

var _ portvolume.VolumeLabelCollector = (*SysfsVolumeLabelCollector)(nil)

// NewSysfsVolumeLabelCollector creates a new SysfsVolumeLabelCollector for the given filesystem path.
func NewSysfsVolumeLabelCollector(path string) (*SysfsVolumeLabelCollector, error) {
	sysDevPath, err := resolveSysfsDevicePath(path)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve sysfs device path: %w", err)
	}

	return &SysfsVolumeLabelCollector{
		path:       path,
		sysDevPath: sysDevPath,
	}, nil
}

// resolveSysfsDevicePath resolves a filesystem path to its sysfs device directory.
//
// Algorithm:
// 1. Stat the path to get device major:minor
// 2. Read symlink /sys/dev/block/major:minor to get device path
// 3. Ascend directory tree while there's a "partition" file (to get to the parent device)
// 4. Return the sysfs device path
func resolveSysfsDevicePath(path string) (string, error) {
	// Get device major:minor from path
	var stat unix.Stat_t
	if err := unix.Stat(path, &stat); err != nil {
		return "", fmt.Errorf("failed to stat path: %w", err)
	}

	major := unix.Major(stat.Dev)
	minor := unix.Minor(stat.Dev)

	// Read symlink to get device path in sysfs
	devLink := fmt.Sprintf("/sys/dev/block/%d:%d", major, minor)
	devPath, err := os.Readlink(devLink)
	if err != nil {
		return "", fmt.Errorf("failed to read device symlink %s: %w", devLink, err)
	}

	// devPath is relative to /sys/dev/block, make it absolute
	devPath = filepath.Join("/sys/dev/block", devPath)
	devPath = filepath.Clean(devPath)

	// Ascend while we're at a partition to get to the parent device
	for {
		partitionPath := filepath.Join(devPath, "partition")

		// Check if this is a partition
		if _, err := os.Stat(partitionPath); os.IsNotExist(err) {
			// No partition file, this is the actual block device
			return devPath, nil
		}

		// This is a partition, ascend to parent
		devPath = filepath.Dir(devPath)
	}
}

// CollectVolumeLabels reads sysfs to collect volume labels.
func (c *SysfsVolumeLabelCollector) CollectVolumeLabels() (map[string]string, error) {
	labels := make(map[string]string)

	// Model
	if model, err := c.readFileTrimSpace("device/model"); err == nil && model != "" {
		labels[LabelModel] = model
	}

	// Vendor
	if vendor, err := c.readFileTrimSpace("device/vendor"); err == nil && vendor != "" {
		labels[LabelVendor] = vendor
	}

	// SSD vs HDD (rotational = 0 = SSD)
	if rotational, err := c.readFileTrimSpace("queue/rotational"); err == nil {
		switch rotational {
		case "0":
			labels[LabelSSD] = "true"
		case "1":
			labels[LabelHDD] = "true"
		}
	}

	// Determine transport/bus
	transportLabel := c.detectTransport()
	if transportLabel != "" {
		labels[transportLabel] = "true"
	}

	return labels, nil
}

// detectTransport determines the transport type from sysfs.
// Returns the label key for the detected transport, or empty string if none detected.
func (c *SysfsVolumeLabelCollector) detectTransport() string {
	// Try device/transport first
	transport, err := c.readFileTrimSpace("device/transport")
	if err != nil {
		// Fallback: read subsystem symlink
		subsystemLink := filepath.Join(c.sysDevPath, "device/subsystem")
		if link, err := os.Readlink(subsystemLink); err == nil {
			transport = filepath.Base(link)
		}
	}

	transport = strings.ToLower(transport)

	switch transport {
	case "nvme":
		return LabelNVMe
	case "iscsi":
		return LabelISCSI
	case "usb":
		return LabelUSB
	case "virtio", "virtio_blk":
		return LabelVirtIO
	case "ata":
		return c.detectATATransport()
	case "scsi":
		return c.detectSCSITransport()
	}

	return ""
}

// detectATATransport distinguishes between SATA and PATA.
func (c *SysfsVolumeLabelCollector) detectATATransport() string {
	driverLink := filepath.Join(c.sysDevPath, "device/driver")
	link, err := os.Readlink(driverLink)
	if err != nil {
		return ""
	}

	driver := filepath.Base(link)

	switch driver {
	case "ahci":
		// AHCI driver => true SATA (Serial ATA)
		return LabelSATA
	default:
		// All other ATA drivers (pata_*, ata_piix, legacy IDE mode)
		// represent PATA (Parallel ATA) or SATA controllers forced into IDE mode.
		return LabelPATA
	}
}

// detectSCSITransport distinguishes between SCSI, SAS, and iSCSI.
func (c *SysfsVolumeLabelCollector) detectSCSITransport() string {
	// Check for iSCSI
	iscsiPath := filepath.Join(c.sysDevPath, "device/iscsi_session")
	if _, err := os.Stat(iscsiPath); err == nil {
		return LabelISCSI
	}

	// Check for SAS
	sasDevicePath := filepath.Join(c.sysDevPath, "device/sas_device")
	if _, err := os.Stat(sasDevicePath); err == nil {
		return LabelSAS
	}

	sasPortPath := filepath.Join(c.sysDevPath, "device/sas_port")
	if _, err := os.Stat(sasPortPath); err == nil {
		return LabelSAS
	}

	// Default to SCSI
	return LabelSCSI
}

// readFileTrimSpace reads a file relative to sysDevPath and returns its content with whitespace trimmed.
func (c *SysfsVolumeLabelCollector) readFileTrimSpace(relPath string) (string, error) {
	fullPath := filepath.Join(c.sysDevPath, relPath)
	data, err := os.ReadFile(fullPath)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

// Close releases resources held by the collector.
func (c *SysfsVolumeLabelCollector) Close() error {
	return nil
}
