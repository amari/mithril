package node

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	infrastructureetcd "github.com/amari/mithril/chunk-node/adapter/infrastructure/etcd"
	"github.com/amari/mithril/chunk-node/domain"
	portnode "github.com/amari/mithril/chunk-node/port/node"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
	"github.com/rs/zerolog"
	clientv3 "go.etcd.io/etcd/client/v3"
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
	if p.nodeLabelMapValue == nil {
		nodeID, err := p.nodeIDProvider.GetNodeID()
		if err != nil {
			return err
		}

		key := "/mithril/cluster/nodes/" + fmt.Sprintf("%08x", nodeID) + "/labels"

		p.nodeLabelMapValue = infrastructureetcd.NewEventualValue(p.client, key)
	}

	//value, err := NodeLabelMap(labels).MarshalMsg(nil)
	value, err := json.Marshal(labels)
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
	if p.volumeLabelIndexValue == nil {
		nodeID, err := p.nodeIDProvider.GetNodeID()
		if err != nil {
			return err
		}

		key := "/mithril/cluster/nodes/" + fmt.Sprintf("%08x", nodeID) + "/volumeLabelIndex"

		p.volumeLabelIndexValue = infrastructureetcd.NewEventualValue(p.client, key)
	}

	model := make(map[string]map[string][]byte, len(volumeIDSetLabelIndexes))

	for labelKey, a := range volumeIDSetLabelIndexes {
		for labelValue, volumeIDSet := range a {
			words := volumeIDSet.Words()
			bytes := make([]byte, 0, 8*len(words))

			for _, word := range words {
				bytes = binary.BigEndian.AppendUint64(bytes, word)
			}

			if _, ok := model[labelKey]; !ok {
				model[labelKey] = map[string][]byte{}
			}

			model[labelKey][labelValue] = bytes
		}
	}

	//value, err := VolumeIDLabelIndexes(model).MarshalMsg(nil)
	value, err := json.Marshal(model)
	if err != nil {
		return err
	}

	// valueStr := base64.StdEncoding.EncodeToString(value)
	valueStr := string(value)
	p.volumeLabelIndexValue.Set(valueStr)

	return nil
}
