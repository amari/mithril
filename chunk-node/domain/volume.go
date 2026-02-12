package domain

type VolumeID uint16

type DirectoryVolume struct {
	VolumeID VolumeID
	Path     string
}

type VolumeState int

const (
	VolumeStateUnknown VolumeState = iota
	VolumeStateOK
	VolumeStateDegraded
	VolumeStateFailed
)

func (vs VolumeState) String() string {
	switch vs {
	case VolumeStateOK:
		return "OK"
	case VolumeStateDegraded:
		return "Degraded"
	case VolumeStateFailed:
		return "Failed"
	default:
		return "Unknown"
	}
}

type VolumeHealth struct {
	State VolumeState
}

// VolumeCharacteristics describes the physical properties of a storage volume.
// These characteristics are detected from platform-specific sources (IOKit on macOS,
// sysfs on Linux) and normalized into a unified representation for labeling and
// observability purposes.
type VolumeCharacteristics struct {
	DeviceName   string
	Medium       MediumType
	Interconnect InterconnectType
	Protocol     ProtocolType
	Vendor       string
	Model        string
	SerialNumber string
}

// MediumType describes the physical storage medium.
// The zero value ("") indicates the medium type could not be determined.
type MediumType string

const (
	// MediumTypeRotational indicates a spinning disk (HDD).
	MediumTypeRotational MediumType = "rotational"
	// MediumTypeSolidState indicates flash storage (SSD).
	MediumTypeSolidState MediumType = "solidstate"
)

// InterconnectType describes the physical or network layer connecting a storage device.
// The zero value ("") indicates the interconnect type could not be determined.
type InterconnectType string

const (
	// InterconnectTypeFibreChannel indicates a Fibre Channel connection.
	InterconnectTypeFibreChannel InterconnectType = "fibrechannel"
	// InterconnectTypeFireWire indicates an IEEE 1394 (FireWire) connection.
	InterconnectTypeFireWire InterconnectType = "firewire"
	// InterconnectTypeInfiniBand indicates an InfiniBand connection.
	InterconnectTypeInfiniBand InterconnectType = "infiniband"
	// InterconnectTypePATA indicates a Parallel ATA (IDE) connection.
	InterconnectTypePATA InterconnectType = "pata"
	// InterconnectTypePCIExpress indicates a PCI Express connection.
	InterconnectTypePCIExpress InterconnectType = "pciexpress"
	// InterconnectTypeRDMA indicates an RDMA connection (e.g., RoCE, iWARP).
	InterconnectTypeRDMA InterconnectType = "rdma"
	// InterconnectTypeSAS indicates a Serial Attached SCSI connection.
	InterconnectTypeSAS InterconnectType = "sas"
	// InterconnectTypeSATA indicates a Serial ATA connection.
	InterconnectTypeSATA InterconnectType = "sata"
	// InterconnectTypeTCP indicates a TCP/IP network connection.
	InterconnectTypeTCP InterconnectType = "tcp"
	// InterconnectTypeUSB indicates a USB connection.
	InterconnectTypeUSB InterconnectType = "usb"
	// InterconnectTypeVirtIO indicates a VirtIO virtual device.
	InterconnectTypeVirtIO InterconnectType = "virtio"
)

// ProtocolType describes the storage command protocol.
// This determines queuing behavior: ATA has a single queue (depth up to 32 with NCQ),
// SCSI has a single deep queue, and NVMe has multiple deep queues.
// The zero value ("") indicates the protocol could not be determined.
type ProtocolType string

const (
	// ProtocolTypeATA indicates the ATA command set (used by SATA/PATA devices).
	ProtocolTypeATA ProtocolType = "ata"
	// ProtocolTypeSCSI indicates the SCSI command set (used by SAS, USB, FireWire, iSCSI, FC).
	ProtocolTypeSCSI ProtocolType = "scsi"
	// ProtocolTypeNVMe indicates the NVMe command set.
	ProtocolTypeNVMe ProtocolType = "nvme"
)
