package domain

import (
	"context"
	"encoding/binary"
	"io"
	"math/bits"
	"math/rand/v2"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

type VolumeID uint16

type VolumeIDCounter interface {
	Next() (VolumeID, error)
}

type VolumeSet struct {
	words  []uint64
	popcnt int
}

func NewVolumeSet() *VolumeSet {
	return &VolumeSet{}
}

func (s *VolumeSet) AddVolume(volume VolumeID) {
	i := int(uint16(volume) / 64)
	j := int(uint16(volume) % 64)
	bit := uint64(1) << j

	words := s.words

	if i >= len(words) {
		if i >= cap(words) {
			s.words = make([]uint64, i+1)
			copy(s.words, words)
		} else {
			s.words = words[:cap(words)]
		}
	}

	word := s.words[i]

	if word&bit != 0 {
		return
	}

	s.words[i] = word | bit
	s.popcnt += 1
}

func (s *VolumeSet) RemoveVolume(volume VolumeID) {
	i := int(uint16(volume) / 64)
	j := int(uint16(volume) % 64)
	bit := uint64(1) << j

	if i >= len(s.words) {
		return
	}

	word := s.words[i]

	if word&bit == 0 {
		return
	}

	s.words[i] = word &^ bit
	s.popcnt -= 1
}

func (s *VolumeSet) ContainsVolume(volume VolumeID) bool {
	i := int(uint16(volume) / 64)
	j := int(uint16(volume) % 64)
	bit := uint64(1) << j

	if i >= len(s.words) {
		return false
	}

	word := s.words[i]

	return (word&bit != 0)
}

func (s *VolumeSet) Count() int {
	return s.popcnt
}

func (s *VolumeSet) MarshalBinary() ([]byte, error) {
	buf := make([]byte, 0, len(s.words)*8)

	for _, word := range s.words {
		buf = binary.LittleEndian.AppendUint64(buf, word)
	}

	return buf, nil
}

func (s *VolumeSet) UnmarshalBinary(data []byte) error {
	full := len(data) / 64
	rem := len(data) % 64

	n := full
	if rem != 0 {
		n++
	}

	if n == 0 {
		s.words = nil
		s.popcnt = 0
		return nil
	}

	s.words = make([]uint64, n)

	popcnt := 0

	for i := range full {
		w := binary.LittleEndian.Uint64(data[:8])
		s.words[i] = w
		popcnt += bits.OnesCount64(w)
		data = data[8:]
	}

	if rem != 0 {
		var buf [8]byte
		copy(buf[:], data)
		w := binary.LittleEndian.Uint64(buf[:])
		s.words[full] = w
		popcnt += bits.OnesCount64(w)
	}

	s.popcnt = popcnt

	return nil
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

	Filesystem FilesystemType
	Zoning     ZoningType
}

// MediumType describes the physical storage medium.
// The zero value ("") indicates the medium type could not be determined.
type MediumType string

const (
	// MediumTypeRotational indicates a spinning disk (HDD).
	MediumTypeRotational MediumType = "rotational"
	// MediumTypeSolidState indicates flash storage (SSD).
	MediumTypeSolidState MediumType = "solid-state"
)

// InterconnectType describes the physical or network layer connecting a storage device.
// The zero value ("") indicates the interconnect type could not be determined.
type InterconnectType string

const (
	// InterconnectTypeFibreChannel indicates a Fibre Channel connection.
	InterconnectTypeFibreChannel InterconnectType = "fibre-channel"
	// InterconnectTypeFireWire indicates an IEEE 1394 (FireWire) connection.
	InterconnectTypeFireWire InterconnectType = "firewire"
	// InterconnectTypeInfiniBand indicates an InfiniBand connection.
	InterconnectTypeInfiniBand InterconnectType = "infiniband"
	// InterconnectTypePATA indicates a Parallel ATA (IDE) connection.
	InterconnectTypePATA InterconnectType = "pata"
	// InterconnectTypePCIExpress indicates a PCI Express connection.
	InterconnectTypePCIExpress InterconnectType = "pci-express"
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

type FilesystemType string

const (
	FilesystemTypeUnknown FilesystemType = ""
	FilesystemTypeAPFS    FilesystemType = "apfs"
	FilesystemTypeBtrfs   FilesystemType = "btrfs"
	FilesystemTypeExt4    FilesystemType = "ext4"
	FilesystemTypeHFSPlus FilesystemType = "hfs+"
	FilesystemTypeNTFS    FilesystemType = "ntfs"
	FilesystemTypeReFS    FilesystemType = "refs"
	FilesystemTypeXFS     FilesystemType = "xfs"
	FilesystemTypeZFS     FilesystemType = "zfs"
)

func (fs FilesystemType) String() string {
	switch fs {
	case FilesystemTypeAPFS:
		return "APFS"
	case FilesystemTypeBtrfs:
		return "Btrfs"
	case FilesystemTypeExt4:
		return "Ext4"
	case FilesystemTypeHFSPlus:
		return "HFS+"
	case FilesystemTypeNTFS:
		return "NTFS"
	case FilesystemTypeReFS:
		return "ReFS"
	case FilesystemTypeXFS:
		return "XFS"
	case FilesystemTypeZFS:
		return "ZFS"
	default:
		return "Unknown"
	}
}

// ZoningType describes the write‑ordering and zone‑management behavior
// required by a storage device. Zoned devices divide media into zones
// with sequential‑write constraints; the zoning type indicates how much
// of this behavior is enforced by the device versus managed by the host.
// The zero value ("") indicates the zoning type could not be determined.
type ZoningType string

const (
	// ZoningTypeUnknown indicates that the zoning behavior could not be detected.
	ZoningTypeUnknown ZoningType = ""

	// ZoningTypeDriveManaged indicates that the device implements zoned media
	// internally but presents a conventional block interface to the host.
	// This corresponds to drive‑managed SMR HDDs.
	ZoningTypeDriveManaged ZoningType = "drive-managed"

	// ZoningTypeHostAware indicates that the device exposes zones to the host
	// and can benefit from sequential writes, but still accepts random writes.
	// This applies to host‑aware SMR HDDs and host‑aware ZNS devices.
	ZoningTypeHostAware ZoningType = "host-aware"

	// ZoningTypeHostManaged indicates that the device requires the host to
	// issue writes sequentially within zones and to manage zone resets.
	// This applies to host‑managed SMR (HM‑SMR), SCSI ZBC, and NVMe ZNS
	// devices operating in host‑managed mode.
	ZoningTypeHostManaged ZoningType = "host-managed"
)

type VolumeCharacteristicsProvider interface {
	Get() *VolumeCharacteristics
	Watch(watchCtx context.Context) <-chan struct{}
}

type VolumeStatus struct {
	Health VolumeHealth
}

type VolumeStatusProvider interface {
	Get() VolumeStatus
	Watch(watchCtx context.Context) <-chan struct{}
}

type VolumeHealth int

const (
	VolumeUnknown VolumeHealth = iota
	VolumeOK
	VolumeDegraded
	VolumeFailed
)

func (h VolumeHealth) String() string {
	switch h {
	case VolumeUnknown:
		return "unknown"
	case VolumeOK:
		return "ok"
	case VolumeDegraded:
		return "degraded"
	case VolumeFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// IsTransient reports whether the volume health represents a temporary,
// recoverable condition. A transient state (Degraded) indicates the volume
// can still serve reads but should avoid writes until it recovers.
func (h VolumeHealth) IsTransient() bool {
	return h == VolumeDegraded
}

// IsPermanent reports whether the volume health represents a terminal,
// non‑recoverable failure. A permanent state (Failed) means the volume
// cannot safely serve any I/O and requires manual intervention or rebuild.
func (h VolumeHealth) IsPermanent() bool {
	return h == VolumeFailed
}

// ShedsWrites reports whether the volume should reject write operations
// due to its current health. Degraded volumes shed writes to avoid
// worsening the condition, and Failed volumes shed both reads and writes.
func (h VolumeHealth) ShedsWrites() bool {
	return h == VolumeDegraded || h == VolumeFailed
}

// ShedsReads reports whether the volume should reject read operations.
// Only Failed volumes shed reads, because data is no longer reliably
// accessible in that state.
func (h VolumeHealth) ShedsReads() bool {
	return h == VolumeFailed
}

type VolumeStatisticsProvider[T any] interface {
	Get() Sample[T]
	Watch(watchCtx context.Context) <-chan struct{}
}

type VolumeLabelSource interface {
	Read() map[string]string
	Watch(watchCtx context.Context) <-chan struct{}
}

type VolumeLabelPublisher interface {
	Publish(node NodeID, labels map[VolumeID]map[string]string)
}

type FileStoreFormat interface {
	Initialize(node NodeID, volume VolumeID, path string) error
	Stat(path string) (*VolumeInfo, error)
	Open(path string) (VolumeHandle, error)
}

type VolumeInfo struct {
	ID   VolumeID
	Node NodeID
}

type VolumeHandle interface {
	io.Closer

	Info() VolumeInfo

	GetChunkStorage() ChunkStorage

	GetCharacteristicsProvider() VolumeCharacteristicsProvider

	GetLabelSources() []VolumeLabelSource

	GetStatusProvider() VolumeStatusProvider

	GetStructuredLoggingFieldsProvider() VolumeStructuredLoggingFieldsProvider

	GetOTelAttributeProvider() VolumeOTelAttributeProvider

	GetIOKitIOBlockStorageDriverStatisticsProvider() VolumeStatisticsProvider[IOKitIOBlockStorageDriverStatistics]

	GetLinuxBlockLayerStatisticsProvider() VolumeStatisticsProvider[LinuxBlockLayerStatistics]

	GetSpaceUtilizationStatisticsProvider() VolumeStatisticsProvider[SpaceUtilizationStatistics]
}

type VolumeOTelAttributeProvider interface {
	Get() []attribute.KeyValue
}

type VolumeStructuredLoggingFieldsProvider interface {
	Get() []any
}

type VolumeChooser interface {
	Choose(options ChooseVolumeOptions) (VolumeID, error)
	Reset(ids []VolumeID)
	Refresh(id VolumeID)
}

type ChooseVolumeOptions struct {
	MaxAttempts int
	Filter      func(id VolumeID) bool
	Probe       func(total, start, attempt int) int
}

// Linear implements a linear probing strategy.
// It calculates the next index to probe based on the total number of items,
// the starting index, and the current attempt number.
func LinearProbe(total, start, attempt int) int {
	if total <= 0 {
		return -1
	}

	return (start + attempt) % total
}

// Quadratic calculates the next index to probe using quadratic probing.
// It takes the total number of slots, the starting index, and the current attempt number.
// It returns the next index to probe, or -1 if total is less than or equal to 0.
func QuadraticProbe(total, start, attempt int) int {
	if total <= 0 {
		return -1
	}

	step := attempt * attempt

	return (start + step) % total
}

// Random selects a random index from the available total number of targets.
// It ignores the start and attempt parameters.
// If total is less than or equal to zero, it returns -1.
func RandomProbe(total, _start, _attempt int) int {
	if total <= 0 {
		return -1
	}

	return rand.IntN(total)
}

// ProbeWithRand returns a probe function that selects a random index
// from the available total number of targets.
// If rng is nil, a new random number generator seeded with the current time is used.
func RandomProbeWithRand(rng *rand.Rand) func(total, start, attempt int) int {
	if rng == nil {
		now := uint64(time.Now().UnixNano())
		rng = rand.New(rand.NewPCG(now, now<<1|1))
	}

	return func(total, _start, _attempt int) int {
		if total <= 0 {
			return -1
		}

		return rng.IntN(total)
	}
}
