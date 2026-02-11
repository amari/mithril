//go:build linux
// +build linux

package linux

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	linuxpkg "github.com/amari/mithril/chunk-node/linux"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
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
	path        string
	blockDevice *linuxpkg.BlockDevice
}

var _ portvolume.VolumeLabelCollector = (*SysfsVolumeLabelCollector)(nil)

// NewSysfsVolumeLabelCollector creates a new SysfsVolumeLabelCollector for the given filesystem path.
func NewSysfsVolumeLabelCollector(path string) (*SysfsVolumeLabelCollector, error) {
	blockDevice, err := linuxpkg.ResolveBlockDevice(path)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve block device: %w", err)
	}

	return &SysfsVolumeLabelCollector{
		path:        path,
		blockDevice: blockDevice,
	}, nil
}

// CollectVolumeLabels reads sysfs to collect volume labels.
func (c *SysfsVolumeLabelCollector) CollectVolumeLabels() (map[string]string, error) {
	labels := make(map[string]string)

	// Model
	if model, err := c.blockDevice.ReadFile("device/model"); err == nil && model != "" {
		labels[LabelModel] = model
	}

	// Vendor
	if vendor, err := c.blockDevice.ReadFile("device/vendor"); err == nil && vendor != "" {
		labels[LabelVendor] = vendor
	}

	// SSD vs HDD (rotational = 0 = SSD)
	if rotational, err := c.blockDevice.ReadFile("queue/rotational"); err == nil {
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
	transport, err := c.blockDevice.ReadFile("device/transport")
	if err != nil {
		// Fallback: read subsystem symlink
		subsystemLink := filepath.Join(c.blockDevice.SysfsPath, "device/subsystem")
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
	driverLink := filepath.Join(c.blockDevice.SysfsPath, "device/driver")
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
	iscsiPath := filepath.Join(c.blockDevice.SysfsPath, "device/iscsi_session")
	if _, err := os.Stat(iscsiPath); err == nil {
		return LabelISCSI
	}

	// Check for SAS
	sasDevicePath := filepath.Join(c.blockDevice.SysfsPath, "device/sas_device")
	if _, err := os.Stat(sasDevicePath); err == nil {
		return LabelSAS
	}

	sasPortPath := filepath.Join(c.blockDevice.SysfsPath, "device/sas_port")
	if _, err := os.Stat(sasPortPath); err == nil {
		return LabelSAS
	}

	// Default to SCSI
	return LabelSCSI
}

// Close releases resources held by the collector.
func (c *SysfsVolumeLabelCollector) Close() error {
	return nil
}
