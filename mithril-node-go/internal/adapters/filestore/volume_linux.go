//go:build linux
// +build linux

package adaptersfilestore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	adapterssysfs "github.com/amari/mithril/mithril-node-go/internal/adapters/sysfs"
	adaptersunix "github.com/amari/mithril/mithril-node-go/internal/adapters/unix"
	"github.com/amari/mithril/mithril-node-go/internal/domain"
)

// Filesystem magic numbers from <linux/magic.h>
const (
	BTRFS_SUPER_MAGIC  = 0x9123683E
	EXT4_SUPER_MAGIC   = 0xEF53
	NTFS_SUPER_MAGIC   = 0x5346544E // note: fs/ntfs/super.c and fs/ntfs3/super.c define this
	XFS_SUPER_MAGIC    = 0x58465342
	ZFS_SUPER_MAGIC    = 0x2FC12FC1 // note: ZFS-on-Linux defines this
	ZONEFS_SUPER_MAGIC = 0x5a4f4653
)

func FilesystemTypeFromPath(path string) (domain.FilesystemType, error) {
	st, err := adaptersunix.Statfs(path)
	if err != nil {
		return "", err
	}

	switch st.Type {
	case BTRFS_SUPER_MAGIC:
		return domain.FilesystemTypeBtrfs, nil
	case EXT4_SUPER_MAGIC:
		return domain.FilesystemTypeExt4, nil
	case NTFS_SUPER_MAGIC:
		return domain.FilesystemTypeNTFS, nil
	case XFS_SUPER_MAGIC:
		return domain.FilesystemTypeXFS, nil
	case ZFS_SUPER_MAGIC:
		return domain.FilesystemTypeZFS, nil
	case ZONEFS_SUPER_MAGIC:
		return "", fmt.Errorf("zoned block devices (HM-SMR, HA-SMR, DM-SMR, NVMe ZNS, etc.) are not yet supported")
	default:
		return "", fmt.Errorf("unsupported filesystem type: 0x%X", st.Type)
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

	p.wg.Go(func() {
		defer close(ch)

		select {
		case <-watchCtx.Done():
		case <-p.syncContext.Done():
		}

		p.mu.Lock()
		delete(p.subscribers, ch)
		p.mu.Unlock()
	})

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
	blockDevice, err := adapterssysfs.ResolveBlockDevice(path)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve block device: %w", err)
	}

	// The sysfs path ends with /block/<devname>, e.g., /sys/devices/.../block/sda
	devName := filepath.Base(blockDevice.SysfsPath)

	characteristics := domain.VolumeCharacteristics{
		DeviceName: "/dev/" + devName,
	}

	readDeviceAttributes(blockDevice, &characteristics)
	detectInterconnectAndProtocol(blockDevice, &characteristics)

	// Detect filesystem
	characteristics.Filesystem, err = FilesystemTypeFromPath(path)
	if err != nil {
		return nil, err
	}

	return &characteristics, nil
}

// readDeviceAttributes reads model, vendor, serial, and medium type from sysfs.
func readDeviceAttributes(blockDevice *adapterssysfs.BlockDevice, characteristics *domain.VolumeCharacteristics) {
	if model, err := blockDevice.ReadFile("device/model"); err == nil && model != "" {
		characteristics.Model = model
	}

	if vendor, err := blockDevice.ReadFile("device/vendor"); err == nil && vendor != "" {
		characteristics.Vendor = vendor
	}

	if serial, err := blockDevice.ReadFile("device/serial"); err == nil && serial != "" {
		characteristics.SerialNumber = serial
	}

	// rotational: 0 = SSD, 1 = HDD
	if rotational, err := blockDevice.ReadFile("queue/rotational"); err == nil {
		switch rotational {
		case "0":
			characteristics.Medium = domain.MediumTypeSolidState
		case "1":
			characteristics.Medium = domain.MediumTypeRotational
		}
	}
}

// detectInterconnectAndProtocol determines the interconnect and protocol from sysfs.
// See package documentation for the detection strategy.
func detectInterconnectAndProtocol(blockDevice *adapterssysfs.BlockDevice, characteristics *domain.VolumeCharacteristics) {
	subsystem := readSubsystem(blockDevice)

	switch subsystem {
	case "nvme":
		detectNVMe(blockDevice, characteristics)
	case "ata":
		tryDetectATAViaPCIDriver(blockDevice, characteristics)
	case "scsi":
		detectSCSI(blockDevice, characteristics)
	case "usb":
		characteristics.Interconnect = domain.InterconnectTypeUSB
		characteristics.Protocol = domain.ProtocolTypeSCSI
	case "virtio":
		characteristics.Interconnect = domain.InterconnectTypeVirtIO
	}
}

// readSubsystem reads the device subsystem from sysfs.
// Returns the basename of the subsystem symlink target (e.g., "nvme", "scsi", "ata").
func readSubsystem(blockDevice *adapterssysfs.BlockDevice) string {
	subsystemLink := filepath.Join(blockDevice.SysfsPath, "device/subsystem")
	if link, err := os.Readlink(subsystemLink); err == nil {
		return strings.ToLower(filepath.Base(link))
	}
	return ""
}

// ----------------------------------------------------------------------------
// NVMe Detection
// ----------------------------------------------------------------------------

// detectNVMe sets the protocol to NVMe and reads device/transport for the interconnect.
// Transport values: pcie (local), tcp/rdma/fc (NVMe-oF), loop (testing).
func detectNVMe(blockDevice *adapterssysfs.BlockDevice, characteristics *domain.VolumeCharacteristics) {
	characteristics.Protocol = domain.ProtocolTypeNVMe

	transport, _ := blockDevice.ReadFile("device/transport")

	switch strings.ToLower(transport) {
	case "pcie":
		characteristics.Interconnect = domain.InterconnectTypePCIExpress
	case "tcp":
		characteristics.Interconnect = domain.InterconnectTypeTCP
	case "rdma":
		characteristics.Interconnect = domain.InterconnectTypeRDMA
	case "fc":
		characteristics.Interconnect = domain.InterconnectTypeFibreChannel
	}
	// Unknown transports (including "loop") leave interconnect empty
}

// ----------------------------------------------------------------------------
// ATA Detection (SATA / PATA)
// ----------------------------------------------------------------------------

// tryDetectATAViaPCIDriver walks up the device tree to find the PCI controller driver
// and classifies the device as SATA or PATA based on the driver name.
//
// This is the only reliable method because libata exposes both SATA and PATA devices
// as SCSI devices, making subsystem-based detection ambiguous.
//
// Returns true if an ATA driver was detected.
func tryDetectATAViaPCIDriver(blockDevice *adapterssysfs.BlockDevice, characteristics *domain.VolumeCharacteristics) bool {
	deviceLink := filepath.Join(blockDevice.SysfsPath, "device")
	devicePath, err := filepath.EvalSymlinks(deviceLink)
	if err != nil {
		return false
	}

	// Walk up the tree to find the first directory with a driver symlink
	for current := devicePath; current != "/" && current != "."; current = filepath.Dir(current) {
		driverLink := filepath.Join(current, "driver")
		if link, err := os.Readlink(driverLink); err == nil {
			return classifyATADriver(filepath.Base(link), characteristics)
		}
	}
	return false
}

// classifyATADriver sets interconnect and protocol based on the PCI driver name.
// Returns true if the driver is an ATA driver (SATA or PATA).
//
// Driver classification:
//   - SATA: ahci, sata_* (sata_nv, sata_sil, sata_via, etc.)
//   - PATA: pata_* (pata_atiixp, pata_marvell, etc.), ide-generic
//   - Ambiguous: ata_piix (defaults to SATA as it's more common)
func classifyATADriver(driver string, characteristics *domain.VolumeCharacteristics) bool {
	driver = strings.ToLower(driver)

	var interconnect domain.InterconnectType

	switch {
	case driver == "ahci":
		interconnect = domain.InterconnectTypeSATA
	case strings.HasPrefix(driver, "sata_"):
		interconnect = domain.InterconnectTypeSATA
	case driver == "ata_piix":
		// Ambiguous: can be SATA or PATA depending on controller mode.
		// Default to SATA as it's more common on systems still in use.
		interconnect = domain.InterconnectTypeSATA
	case strings.HasPrefix(driver, "pata_"):
		interconnect = domain.InterconnectTypePATA
	case driver == "ide-generic":
		interconnect = domain.InterconnectTypePATA
	default:
		return false
	}

	characteristics.Interconnect = interconnect
	characteristics.Protocol = domain.ProtocolTypeATA
	return true
}

// ----------------------------------------------------------------------------
// SCSI Detection (SAS, iSCSI, FC, USB, FireWire, VirtIO-SCSI, and ATA via libata)
// ----------------------------------------------------------------------------

// detectSCSI handles the SCSI subsystem, which is a catch-all for many device types.
//
// Detection order (most authoritative first):
//  1. device/transport file
//  2. PCI driver inspection (catches ATA devices exposed via libata)
//  3. iscsi_session directory
//  4. sas_device / sas_port directories
//  5. Device path inspection (fallback)
func detectSCSI(blockDevice *adapterssysfs.BlockDevice, characteristics *domain.VolumeCharacteristics) {
	characteristics.Protocol = domain.ProtocolTypeSCSI

	// 1. Check device/transport (most authoritative when present)
	if detectSCSIViaTransport(blockDevice, characteristics) {
		return
	}

	// 2. Check for ATA devices exposed via libata
	if tryDetectATAViaPCIDriver(blockDevice, characteristics) {
		return
	}

	// 3. Check for iSCSI via iscsi_session directory
	// TODO: differentiate iSCSI (over TCP) vs iSER (iSCSI over RDMA)
	iscsiPath := filepath.Join(blockDevice.SysfsPath, "device/iscsi_session")
	if _, err := os.Stat(iscsiPath); err == nil {
		characteristics.Interconnect = domain.InterconnectTypeTCP
		return
	}

	// 4. Check for SAS via sas_device or sas_port directories
	sasDevicePath := filepath.Join(blockDevice.SysfsPath, "device/sas_device")
	sasPortPath := filepath.Join(blockDevice.SysfsPath, "device/sas_port")
	if _, err := os.Stat(sasDevicePath); err == nil {
		characteristics.Interconnect = domain.InterconnectTypeSAS
		return
	}
	if _, err := os.Stat(sasPortPath); err == nil {
		characteristics.Interconnect = domain.InterconnectTypeSAS
		return
	}

	// 5. Fallback: inspect device path
	detectSCSIViaDevicePath(blockDevice, characteristics)
}

// detectSCSIViaTransport reads device/transport and sets interconnect accordingly.
// Returns true if transport was detected and handled.
//
// Transport values:
//   - iscsi_transport: iSCSI (SCSI over TCP)
//   - sas_transport: SAS
//   - fc_transport: Fibre Channel
//   - ata_transport: SATA/PATA via libata (protocol is ATA, not SCSI)
func detectSCSIViaTransport(blockDevice *adapterssysfs.BlockDevice, characteristics *domain.VolumeCharacteristics) bool {
	transport, err := blockDevice.ReadFile("device/transport")
	if err != nil {
		return false
	}

	switch strings.ToLower(transport) {
	case "iscsi_transport":
		characteristics.Interconnect = domain.InterconnectTypeTCP
	case "sas_transport":
		characteristics.Interconnect = domain.InterconnectTypeSAS
	case "fc_transport":
		characteristics.Interconnect = domain.InterconnectTypeFibreChannel
	case "ata_transport":
		// SATA/PATA exposed via libata - override protocol
		characteristics.Protocol = domain.ProtocolTypeATA
		tryDetectATAViaPCIDriver(blockDevice, characteristics)
	default:
		return false
	}
	return true
}

// detectSCSIViaDevicePath inspects the resolved device path to infer the interconnect.
// This is a last-resort fallback when transport and directory-based detection fail.
//
// Path patterns:
//   - /usb: USB mass storage
//   - /firewire or /fw: FireWire (IEEE 1394)
//   - /virtio: VirtIO-SCSI
//   - /ata or /ataX: SATA/PATA via libata
//   - /pci: Direct PCI-e device (protocol unknown)
func detectSCSIViaDevicePath(blockDevice *adapterssysfs.BlockDevice, characteristics *domain.VolumeCharacteristics) {
	deviceLink := filepath.Join(blockDevice.SysfsPath, "device")
	devicePath, err := filepath.EvalSymlinks(deviceLink)
	if err != nil {
		return
	}
	devicePath = strings.ToLower(devicePath)

	switch {
	case strings.Contains(devicePath, "/usb"):
		characteristics.Interconnect = domain.InterconnectTypeUSB

	case strings.Contains(devicePath, "/firewire") || strings.Contains(devicePath, "/fw"):
		characteristics.Interconnect = domain.InterconnectTypeFireWire

	case strings.Contains(devicePath, "/virtio"):
		characteristics.Interconnect = domain.InterconnectTypeVirtIO

	case containsATAPath(devicePath):
		characteristics.Protocol = domain.ProtocolTypeATA
		tryDetectATAViaPCIDriver(blockDevice, characteristics)

	case strings.Contains(devicePath, "/pci"):
		// Direct PCI-e device with unknown protocol
		characteristics.Interconnect = domain.InterconnectTypePCIExpress
		characteristics.Protocol = ""
	}
}

// containsATAPath checks if the device path contains an ATA path segment.
// Matches /ata, /ata/, /ata1, /ata12, etc. but not /sata or /pata.
func containsATAPath(path string) bool {
	idx := strings.Index(path, "/ata")
	if idx == -1 {
		return false
	}

	rest := path[idx+4:] // after "/ata"
	if len(rest) == 0 {
		return true
	}

	// Must be followed by a digit, "/" or end of string
	return rest[0] == '/' || (rest[0] >= '0' && rest[0] <= '9')
}
