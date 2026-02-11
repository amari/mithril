//go:build darwin
// +build darwin

// Package darwin provides IOKit utilities for interacting with macOS storage devices.
//
// This package wraps CoreFoundation and IOKit C APIs to enable Go code to:
//   - Discover IOMedia services from filesystem paths or BSD names
//   - Walk the IOService registry tree
//   - Read properties from IOBlockStorageDriver and IOBlockStorageDevice
//   - Convert CoreFoundation types to Go types
//
// # IOKit Physical Interconnect Types
//
// The IOStorageProtocolCharacteristics.h header defines constants for the
// "Physical Interconnect" property found in Protocol Characteristics dictionaries.
// These are the canonical values IOKit uses to identify storage transport types:
//
//	Constant                                        String Value        Suggested Label
//	────────────────────────────────────────────────────────────────────────────────────
//	kIOPropertyPhysicalInterconnectTypeATA          "ATA"               pata=true
//	kIOPropertyPhysicalInterconnectTypeATAPI        "ATAPI"             atapi=true or pata=true
//	kIOPropertyPhysicalInterconnectTypeFibreChannel "Fibre Channel"     fc=true
//	kIOPropertyPhysicalInterconnectTypeFireWire     "FireWire"          firewire=true
//	kIOPropertyPhysicalInterconnectTypeSCSIParallel "SCSI Parallel"     scsi=true
//	kIOPropertyPhysicalInterconnectTypeSecureDigital"Secure Digital"    sd=true
//	kIOPropertyPhysicalInterconnectTypeSerialATA    "SATA"              sata=true
//	kIOPropertyPhysicalInterconnectTypeSerialAttachedSCSI "SAS"         sas=true
//	kIOPropertyPhysicalInterconnectTypeUSB          "USB"               usb=true
//	kIOPropertyPhysicalInterconnectTypeVirtual      "Virtual"           virtio=true
//
// Additionally, kIOPropertyPhysicalInterconnectTypeKey is the dictionary key
// name itself: "Physical Interconnect".
//
// # Future Improvements
//
// The current transport detection in the labeler uses IOService class name
// conformance checks (e.g., IONVMeBlockStorageDevice, IOUSBDevice, IOAHCIPort).
// A more robust approach would be:
//
//  1. Check the "Physical Interconnect" property in Protocol Characteristics
//     on IOBlockStorageDevice as the primary detection method
//  2. Walk up the IOService tree looking for this property on ancestors
//  3. Fall back to class name checks only for transports that don't populate
//     the Physical Interconnect property (e.g., NVMe)
//
// This would:
//   - Be more accurate and aligned with IOKit's reporting
//   - Distinguish SATA (interconnect) from AHCI (controller interface)
//   - Add support for Fibre Channel, FireWire, and SD card detection
//   - Reduce dependence on driver-specific class names
package darwin
