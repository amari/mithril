package adapternode

import (
	"encoding/binary"
	"fmt"

	infrastructureetcd "github.com/amari/mithril/chunk-node/adapter/infrastructure/etcd"
	"github.com/amari/mithril/chunk-node/domain"
	portnode "github.com/amari/mithril/chunk-node/port/node"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
	nodev1 "github.com/amari/mithril/gen/go/proto/mithril/cluster/node/v1"
	"github.com/rs/zerolog"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/encoding/protojson"
)

type EtcdClusterPublisher struct {
	client         *clientv3.Client
	nodeIDProvider portnode.NodeIDProvider
	log            *zerolog.Logger

	leasedMap *infrastructureetcd.Map

	nodeLabelMapValue     *infrastructureetcd.EventualValue
	volumeLabelIndexValue *infrastructureetcd.EventualValue
}

var (
	//_ port.NodeAnnouncer                          = (*EtcdClusterPublisher)(nil)
	_ portnode.NodeLabelPublisher                 = (*EtcdClusterPublisher)(nil)
	_ portvolume.VolumeIDSetLabelIndexesPublisher = (*EtcdClusterPublisher)(nil)
)

func NewClusterPublisher(client *clientv3.Client, nodeIDProvider portnode.NodeIDProvider, log *zerolog.Logger) *EtcdClusterPublisher {
	return &EtcdClusterPublisher{
		client:         client,
		nodeIDProvider: nodeIDProvider,
		log:            log,
	}
}

// PublishNodeLabels implements NodeLabelPublisher.
func (p *EtcdClusterPublisher) PublishNodeLabels(labels map[string]string) error {
	nodeID, err := p.nodeIDProvider.GetNodeID()
	if err != nil {
		return err
	}

	if p.nodeLabelMapValue == nil {
		nodeID, err := p.nodeIDProvider.GetNodeID()
		if err != nil {
			return err
		}

		key := fmt.Sprintf("/registry/nodes/%08x/labels", nodeID)

		p.nodeLabelMapValue = infrastructureetcd.NewEventualValue(p.client, key)
	}

	index := nodev1.Labels{
		NodeId: uint32(nodeID),
	}

	for labelKey, labelValue := range labels {
		index.Labels = append(index.Labels, &nodev1.Labels_Label{
			Key:   labelKey,
			Value: labelValue,
		})
	}

	value, err := protojson.Marshal(&index)
	if err != nil {
		return err
	}

	//valueStr := base64.StdEncoding.EncodeToString(value)
	valueStr := string(value)

	p.nodeLabelMapValue.Set(valueStr)

	return nil
}

// PublishVolumeIDSetLabelIndexes implements VolumeIDSetLabelIndexesPublisher.
func (p *EtcdClusterPublisher) PublishVolumeIDSetLabelIndexes(volumeIDSetLabelIndexes map[string]map[string]*domain.VolumeIDSet) error {
	nodeID, err := p.nodeIDProvider.GetNodeID()
	if err != nil {
		return err
	}

	if p.volumeLabelIndexValue == nil {
		nodeID, err := p.nodeIDProvider.GetNodeID()
		if err != nil {
			return err
		}

		key := fmt.Sprintf("/registry/nodes/%08x/labeled_volume_bitsets", nodeID)

		p.volumeLabelIndexValue = infrastructureetcd.NewEventualValue(p.client, key)
	}

	index := nodev1.LabeledVolumeBitsets{
		NodeId: uint32(nodeID),
	}

	for labelKey, a := range volumeIDSetLabelIndexes {
		for labelValue, volumeIDSet := range a {
			words := volumeIDSet.Words()
			bytes := make([]byte, 0, 8*len(words))

			for _, word := range words {
				bytes = binary.LittleEndian.AppendUint64(bytes, word)
			}

			index.Bitsets = append(index.Bitsets, &nodev1.LabeledVolumeBitsets_LabeledVolumeBitset{
				Key:       labelKey,
				Value:     labelValue,
				VolumeIds: bytes,
			})
		}
	}

	value, err := protojson.Marshal(&index)
	if err != nil {
		return err
	}

	//valueStr := base64.StdEncoding.EncodeToString(value)
	valueStr := string(value)
	p.volumeLabelIndexValue.Set(valueStr)

	return nil
}
