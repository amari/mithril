//go:build darwin
// +build darwin

package adaptersfilestore

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/amari/mithril/mithril-node-go/internal/adapters/iokit"
	adaptersunix "github.com/amari/mithril/mithril-node-go/internal/adapters/unix"
	"github.com/amari/mithril/mithril-node-go/internal/domain"
	"golang.org/x/sys/unix"
)

func FilesystemTypeFromPath(path string) (domain.FilesystemType, error) {
	st, err := adaptersunix.Statfs(path)
	if err != nil {
		return "", err
	}

	fsTypeName := unix.ByteSliceToString(st.Fstypename[:])

	switch strings.ToLower(fsTypeName) {
	case "apfs":
		return domain.FilesystemTypeAPFS, nil
	case "hfs":
		return domain.FilesystemTypeHFSPlus, nil
	case "ntfs":
		return domain.FilesystemTypeNTFS, nil
	case "zfs":
		return domain.FilesystemTypeZFS, nil
	default:
		return "", fmt.Errorf("unsupported filesystem type: %q", fsTypeName)
	}
}

type VolumeCharacteristicsProvider struct {
	path string

	mu                    sync.RWMutex
	wg                    sync.WaitGroup
	characteristics       *domain.VolumeCharacteristics
	syncContext           context.Context
	syncContextCancelFunc context.CancelFunc
	subscribers           map[chan struct{}]struct{}
}

var _ domain.VolumeCharacteristicsProvider = (*VolumeCharacteristicsProvider)(nil)

func NewVolumeCharacteristicsProvider(path string) *VolumeCharacteristicsProvider {
	return &VolumeCharacteristicsProvider{
		path: path,
	}
}

func (p *VolumeCharacteristicsProvider) Get() *domain.VolumeCharacteristics {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.characteristics
}

func (p *VolumeCharacteristicsProvider) Watch(watchCtx context.Context) <-chan struct{} {
	ch := make(chan struct{}, 1)

	p.mu.Lock()
	defer p.mu.Unlock()
	p.subscribers[ch] = struct{}{}

	if p.characteristics != nil {
		select {
		case ch <- struct{}{}:
		default:
		}
	}

	go func() {
		defer close(ch)

		select {
		case <-watchCtx.Done():
		case <-p.syncContext.Done():
		}

		p.mu.Lock()
		delete(p.subscribers, ch)
		p.mu.Unlock()
	}()

	return ch
}

func (p *VolumeCharacteristicsProvider) start() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	characteristics, err := readCharacteristics(p.path)
	if err != nil {
		return err
	}
	p.characteristics = characteristics

	p.subscribers = make(map[chan struct{}]struct{})
	p.syncContext, p.syncContextCancelFunc = context.WithCancel(context.Background())

	return nil
}

func (p *VolumeCharacteristicsProvider) stop() error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	p.syncContextCancelFunc()

	p.wg.Wait()

	p.subscribers = nil

	return nil
}

func readCharacteristics(path string) (*domain.VolumeCharacteristics, error) {
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
	characteristics.Filesystem, err = FilesystemTypeFromPath(path)
	if err != nil {
		return nil, err
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
