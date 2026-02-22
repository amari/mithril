package adaptervolumecharacteristics

import (
	"context"
	"strings"

	"github.com/amari/mithril/chunk-node/domain"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
)

type LabelProvider struct {
	labels map[string]string
}

var _ portvolume.VolumeLabelProvider = (*LabelProvider)(nil)

func NewLabelProvider(characteristics *domain.VolumeCharacteristics) *LabelProvider {
	return &LabelProvider{
		labels: buildVolumeLabels(characteristics),
	}
}

func (p *LabelProvider) GetVolumeLabels() map[string]string {
	return p.labels
}

func (p *LabelProvider) Watch(watchCtx context.Context) <-chan struct{} {
	ch := make(chan struct{})
	close(ch)

	return ch
}

func buildVolumeLabels(characteristics *domain.VolumeCharacteristics) map[string]string {
	if characteristics == nil {
		return nil
	}

	labels := make(map[string]string)

	// Medium labels
	switch characteristics.Medium {
	case domain.MediumTypeRotational:
		labels["hdd"] = "true"
	case domain.MediumTypeSolidState:
		labels["ssd"] = "true"
	}

	// Protocol labels
	switch characteristics.Protocol {
	case domain.ProtocolTypeNVMe:
		labels["nvme"] = "true"
	case domain.ProtocolTypeSCSI:
		labels["scsi"] = "true"
	case domain.ProtocolTypeATA:
		labels["ata"] = "true"
	}

	// Interconnect labels
	switch characteristics.Interconnect {
	case domain.InterconnectTypeFibreChannel:
		labels["fibre-channel"] = "true"
	case domain.InterconnectTypeFireWire:
		labels["firewire"] = "true"
	case domain.InterconnectTypeInfiniBand:
		labels["infiniband"] = "true"
	case domain.InterconnectTypePATA:
		labels["pata"] = "true"
	case domain.InterconnectTypePCIExpress:
		labels["pcie"] = "true"
	case domain.InterconnectTypeRDMA:
		labels["rdma"] = "true"
	case domain.InterconnectTypeSAS:
		labels["sas"] = "true"
	case domain.InterconnectTypeSATA:
		labels["sata"] = "true"
	case domain.InterconnectTypeTCP:
		labels["tcp"] = "true"
	case domain.InterconnectTypeUSB:
		labels["usb"] = "true"
	case domain.InterconnectTypeVirtIO:
		labels["virtio"] = "true"
	}

	// Filesystem labels
	if characteristics.FileSystem != "" {
		labels[strings.ToLower(characteristics.FileSystem.String())] = "true"
	}

	return labels
}
