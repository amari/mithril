//go:build darwin
// +build darwin

package darwin

import (
	"strings"

	iokit "github.com/amari/mithril/chunk-node/darwin"
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
)

// Other label key constants.
const (
	LabelModel  = "model"
	LabelVendor = "vendor"
	LabelSSD    = "ssd"
	LabelHDD    = "hdd"
)

// IOKitVolumeLabelCollector collects volume labels by walking the IOKit registry.
type IOKitVolumeLabelCollector struct {
	path  string
	media *iokit.IOMedia
}

var _ portvolume.VolumeLabelCollector = (*IOKitVolumeLabelCollector)(nil)

// NewIOKitVolumeLabelCollector creates a new IOKitVolumeLabelCollector for the given filesystem path.
func NewIOKitVolumeLabelCollector(path string) (*IOKitVolumeLabelCollector, error) {
	media, err := iokit.IOMediaFromPath(path)
	if err != nil {
		return nil, err
	}

	return &IOKitVolumeLabelCollector{
		path:  path,
		media: media,
	}, nil
}

// CollectVolumeLabels walks the IOService tree from the IOMedia to the root,
// detecting the transport type and collecting relevant labels.
func (c *IOKitVolumeLabelCollector) CollectVolumeLabels() (map[string]string, error) {
	labels := make(map[string]string)

	// Read Device Characteristics for medium type, vendor, model
	driver, err := iokit.GetBlockStorageDriverFromMedia(c.media)
	if err == nil {
		defer driver.Close()

		device, err := iokit.GetBlockStorageDeviceFromDriver(driver)
		if err == nil {
			defer device.Close()

			deviceChars, err := device.ReadRawDeviceCharacteristics()
			if err == nil {
				// Medium Type: "Rotational" or "Solid State"
				if mediumType, ok := deviceChars["Medium Type"].(string); ok {
					switch mediumType {
					case "Rotational":
						labels[LabelHDD] = "true"
					case "Solid State":
						labels[LabelSSD] = "true"
					}
				}

				// Vendor Name
				if vendor, ok := deviceChars["Vendor Name"].(string); ok {
					if v := strings.TrimSpace(vendor); v != "" {
						labels[LabelVendor] = v
					}
				}

				// Product Name -> model
				if product, ok := deviceChars["Product Name"].(string); ok {
					if m := strings.TrimSpace(product); m != "" {
						labels[LabelModel] = m
					}
				}
			}
		}
	}

	// Detect transport by checking ancestor conformance in priority order
	transportLabel, err := c.detectTransport()
	if err != nil {
		return labels, err
	}

	if transportLabel != "" {
		labels[transportLabel] = "true"
	}

	return labels, nil
}

// detectTransport checks ancestor conformance in the exact order specified.
// Returns the label key for the detected transport, or empty string if none detected.
func (c *IOKitVolumeLabelCollector) detectTransport() (string, error) {
	service := c.media.Service()

	// 1. Check for NVMe
	if conforms, err := iokit.CheckIOServiceTreeConformsToClassName(service, "IONVMeBlockStorageDevice"); err != nil {
		return "", err
	} else if conforms {
		return LabelNVMe, nil
	}

	// 2. Check for USB
	if conforms, err := iokit.CheckIOServiceTreeConformsToClassName(service, "IOUSBDevice"); err != nil {
		return "", err
	} else if conforms {
		return LabelUSB, nil
	}

	// 3. Check for VirtIO
	if conforms, err := iokit.CheckIOServiceTreeConformsToClassName(service, "AppleVirtIOStorageDevice"); err != nil {
		return "", err
	} else if conforms {
		return LabelVirtIO, nil
	}

	// 4. Check for AHCI
	if conforms, err := iokit.CheckIOServiceTreeConformsToClassName(service, "IOAHCIPort"); err != nil {
		return "", err
	} else if conforms {
		return LabelSATA, nil
	}

	// 5. Check for PATA
	if conforms, err := iokit.CheckIOServiceTreeConformsToClassName(service, "IOATADevice"); err != nil {
		return "", err
	} else if conforms {
		return LabelPATA, nil
	}

	// 6. Check for SAS via Protocol Characteristics on IOBlockStorageDevice
	driver, err := iokit.GetBlockStorageDriverFromMedia(c.media)
	if err == nil {
		defer driver.Close()

		device, err := iokit.GetBlockStorageDeviceFromDriver(driver)
		if err == nil {
			defer device.Close()

			protocolChars, err := device.ReadRawProtocolCharacteristics()
			if err == nil {
				if physicalInterconnect, ok := protocolChars["Physical Interconnect"].(string); ok && physicalInterconnect == "SAS" {
					return LabelSAS, nil
				}
			}
		}
	}
	// Walk up the IOService registry to find "Physical Interconnect" == "SAS" on an IOService
	if hasSAS, err := iokit.CheckIOServiceTreeHasPhysicalInterconnectEqualToSAS(service); err != nil {
		return "", err
	} else if hasSAS {
		return LabelSAS, nil
	}

	// 7. Check for SCSI (multiple class names)
	if conforms, err := iokit.CheckIOServiceTreeConformsToClassName(service, "IOSCSIProtocolInterface"); err != nil {
		return "", err
	} else if conforms {
		return LabelSCSI, nil
	}

	if conforms, err := iokit.CheckIOServiceTreeConformsToClassName(service, "IOSCSIParallelInterfaceController"); err != nil {
		return "", err
	} else if conforms {
		return LabelSCSI, nil
	}

	if conforms, err := iokit.CheckIOServiceTreeConformsToClassName(service, "IOSCSIDevice"); err != nil {
		return "", err
	} else if conforms {
		return LabelSCSI, nil
	}

	return "", nil
}

// Close releases resources held by the collector.
func (c *IOKitVolumeLabelCollector) Close() error {
	if c.media != nil {
		return c.media.Close()
	}
	return nil
}
