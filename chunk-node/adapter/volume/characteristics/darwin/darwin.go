//go:build darwin
// +build darwin

// Package darwin detects volume characteristics from IOKit on macOS.
//
// # Detection Strategy
//
// macOS exposes storage device information through the IOKit registry. The detection
// process involves:
//
//  1. Resolve the filesystem path to a BSD device name (e.g., "disk0s1")
//  2. Find the IOMedia object for that device in the IOKit registry
//  3. Walk up the IOService tree to find IOBlockStorageDevice
//  4. Read Device Characteristics (medium type, vendor, model, serial)
//  5. Read Protocol Characteristics (physical interconnect)
//
// # Interconnect Detection Order
//
// The interconnect and protocol are detected by checking the IOService tree for known
// class names, then falling back to the "Physical Interconnect" property. The order
// matters because class conformance is more reliable than property inspection:
//
//  1. IONVMeBlockStorageDevice → PCIExpress + NVMe
//  2. IOUSBDevice → USB + SCSI
//  3. AppleVirtIOStorageDevice → VirtIO
//  4. IOAHCIPort → SATA + ATA
//  5. IOATADevice → PATA + ATA
//  6. Protocol Characteristics "Physical Interconnect" property (fallback)
//
// # Why Class Conformance First?
//
// The "Physical Interconnect" property in Protocol Characteristics is metadata set by
// the device driver. Some third-party drivers (especially iSCSI initiators) report
// incorrect values like "SCSI Parallel Interface" instead of the actual transport.
// Checking IOKit class conformance first gives us reliable detection for common cases.
//
// # Apple Silicon Note
//
// On Apple Silicon Macs, the internal SSD reports "Apple Fabric" as its Physical
// Interconnect. This is collapsed to PCIExpress + NVMe since Apple Fabric is
// effectively a PCIe-based fabric and the device speaks NVMe.
package darwin

import (
	"strings"

	iokit "github.com/amari/mithril/chunk-node/darwin"
	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/unix"
)

// GetVolumeCharacteristicsForPath returns the volume characteristics for the given filesystem path.
func GetVolumeCharacteristicsForPath(path string) (*domain.VolumeCharacteristics, error) {
	bsdName, err := iokit.BSDNameFromPath(path)
	if err != nil {
		return nil, err
	}

	media, err := iokit.IOMediaFromBSDName(bsdName)
	if err != nil {
		return nil, err
	}
	defer media.Close()

	characteristics := domain.VolumeCharacteristics{
		DeviceName: "/dev/" + bsdName,
	}

	// Read device and protocol characteristics from IOBlockStorageDevice
	deviceChars, protocolChars := readBlockStorageDeviceProperties(media)

	if deviceChars != nil {
		readDeviceAttributes(deviceChars, &characteristics)
	}

	// Detect interconnect and protocol
	if err := detectInterconnectAndProtocol(media, protocolChars, &characteristics); err != nil {
		return nil, err
	}

	// Detect filesystem
	st, err := unix.Statfs(path)
	if err != nil {
		return nil, err
	}

	switch strings.ToLower(strings.Trim(string(st.Fstypename[:]), "\x00")) {
	case "apfs":
		characteristics.FileSystemType = domain.FileSystemTypeAPFS
	case "hfs":
		characteristics.FileSystemType = domain.FileSystemTypeHFSPlus
	case "ntfs":
		characteristics.FileSystemType = domain.FileSystemTypeNTFS
	case "zfs":
		characteristics.FileSystemType = domain.FileSystemTypeZFS
	}

	return &characteristics, nil
}

// readBlockStorageDeviceProperties walks up the IOService tree from IOMedia to find
// IOBlockStorageDevice and reads its Device Characteristics and Protocol Characteristics.
func readBlockStorageDeviceProperties(media *iokit.IOMedia) (deviceChars, protocolChars map[string]any) {
	driver, err := iokit.GetBlockStorageDriverFromMedia(media)
	if err != nil {
		return nil, nil
	}
	defer driver.Close()

	device, err := iokit.GetBlockStorageDeviceFromDriver(driver)
	if err != nil {
		return nil, nil
	}
	defer device.Close()

	deviceChars, _ = device.ReadRawDeviceCharacteristics()
	protocolChars, _ = device.ReadRawProtocolCharacteristics()

	return deviceChars, protocolChars
}

// readDeviceAttributes extracts medium type, vendor, model, and serial number
// from Device Characteristics.
func readDeviceAttributes(deviceChars map[string]any, characteristics *domain.VolumeCharacteristics) {
	// Medium Type: "Rotational" or "Solid State"
	if mediumType, ok := deviceChars["Medium Type"].(string); ok {
		switch mediumType {
		case "Rotational":
			characteristics.Medium = domain.MediumTypeRotational
		case "Solid State":
			characteristics.Medium = domain.MediumTypeSolidState
		}
	}

	if vendor, ok := deviceChars["Vendor Name"].(string); ok {
		if v := strings.TrimSpace(vendor); v != "" {
			characteristics.Vendor = v
		}
	}

	if product, ok := deviceChars["Product Name"].(string); ok {
		if m := strings.TrimSpace(product); m != "" {
			characteristics.Model = m
		}
	}

	if serial, ok := deviceChars["Serial Number"].(string); ok {
		if s := strings.TrimSpace(serial); s != "" {
			characteristics.SerialNumber = s
		}
	}
}

// ----------------------------------------------------------------------------
// Interconnect and Protocol Detection
// ----------------------------------------------------------------------------

// detectInterconnectAndProtocol determines the interconnect and protocol by checking
// IOService tree class conformance first, then falling back to Protocol Characteristics.
func detectInterconnectAndProtocol(
	media *iokit.IOMedia,
	protocolChars map[string]any,
	characteristics *domain.VolumeCharacteristics,
) error {
	service := media.Service()

	// Try class conformance checks first (most reliable)
	detected, err := detectViaClassConformance(service, characteristics)
	if err != nil {
		return err
	}
	if detected {
		return nil
	}

	// Fall back to Protocol Characteristics property
	detectViaProtocolCharacteristics(protocolChars, characteristics)

	return nil
}

// detectViaClassConformance checks if the IOService tree contains known device classes.
// Returns true if a match was found.
//
// Detection order:
//  1. IONVMeBlockStorageDevice → NVMe over PCIe
//  2. IOUSBDevice → USB mass storage (speaks SCSI)
//  3. AppleVirtIOStorageDevice → VirtIO block device
//  4. IOAHCIPort → SATA (AHCI controller)
//  5. IOATADevice → PATA (legacy ATA)
func detectViaClassConformance(service iokit.IOService, characteristics *domain.VolumeCharacteristics) (bool, error) {
	checks := []struct {
		className    string
		interconnect domain.InterconnectType
		protocol     domain.ProtocolType
	}{
		{"IONVMeBlockStorageDevice", domain.InterconnectTypePCIExpress, domain.ProtocolTypeNVMe},
		{"IOUSBDevice", domain.InterconnectTypeUSB, domain.ProtocolTypeSCSI},
		{"AppleVirtIOStorageDevice", domain.InterconnectTypeVirtIO, ""},
		{"IOAHCIPort", domain.InterconnectTypeSATA, domain.ProtocolTypeATA},
		{"IOATADevice", domain.InterconnectTypePATA, domain.ProtocolTypeATA},
	}

	for _, check := range checks {
		conforms, err := iokit.CheckIOServiceTreeConformsToClassName(service, check.className)
		if err != nil {
			return false, err
		}
		if conforms {
			characteristics.Interconnect = check.interconnect
			if check.protocol != "" {
				characteristics.Protocol = check.protocol
			}
			return true, nil
		}
	}

	return false, nil
}

// detectViaProtocolCharacteristics uses the "Physical Interconnect" property from
// Protocol Characteristics as a fallback when class conformance checks don't match.
//
// Physical Interconnect values:
//   - "Apple Fabric": Apple Silicon internal SSD (treated as PCIe + NVMe)
//   - "ATA": Parallel ATA
//   - "Fibre Channel Interface": Fibre Channel
//   - "FireWire": IEEE 1394
//   - "PCI-Express": PCIe (typically NVMe)
//   - "SAS": Serial Attached SCSI
//   - "SATA": Serial ATA
//   - "SCSI Parallel Interface": Parallel SCSI (or misreported iSCSI from 3rd party drivers)
//   - "USB": USB mass storage
//   - "VirtIO": VirtIO block device
func detectViaProtocolCharacteristics(protocolChars map[string]any, characteristics *domain.VolumeCharacteristics) {
	if protocolChars == nil {
		return
	}

	physicalInterconnect, ok := protocolChars["Physical Interconnect"].(string)
	if !ok {
		return
	}

	switch physicalInterconnect {
	case "Apple Fabric":
		// Apple Silicon internal SSD - Apple Fabric is PCIe-based
		characteristics.Interconnect = domain.InterconnectTypePCIExpress
		characteristics.Protocol = domain.ProtocolTypeNVMe

	case "ATA":
		characteristics.Interconnect = domain.InterconnectTypePATA
		characteristics.Protocol = domain.ProtocolTypeATA

	case "Fibre Channel Interface":
		characteristics.Interconnect = domain.InterconnectTypeFibreChannel
		characteristics.Protocol = domain.ProtocolTypeSCSI

	case "FireWire":
		characteristics.Interconnect = domain.InterconnectTypeFireWire
		characteristics.Protocol = domain.ProtocolTypeSCSI

	case "PCI-Express":
		characteristics.Interconnect = domain.InterconnectTypePCIExpress
		characteristics.Protocol = domain.ProtocolTypeNVMe

	case "SAS":
		characteristics.Interconnect = domain.InterconnectTypeSAS
		characteristics.Protocol = domain.ProtocolTypeSCSI

	case "SATA":
		characteristics.Interconnect = domain.InterconnectTypeSATA
		characteristics.Protocol = domain.ProtocolTypeATA

	case "SCSI Parallel Interface":
		// Note: Some third-party iSCSI initiators incorrectly report this value.
		// We only set the protocol; interconnect remains unknown.
		characteristics.Protocol = domain.ProtocolTypeSCSI

	case "USB":
		characteristics.Interconnect = domain.InterconnectTypeUSB
		characteristics.Protocol = domain.ProtocolTypeSCSI

	case "VirtIO":
		// macOS only supports virtio-blk, not virtio-scsi
		characteristics.Interconnect = domain.InterconnectTypeVirtIO
	}
}
