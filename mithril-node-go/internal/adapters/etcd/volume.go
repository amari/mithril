package adaptersetcd

import (
	"fmt"
	"sync"

	nodev1 "github.com/amari/mithril/gen/go/proto/mithril/cluster/node/v1"
	"github.com/amari/mithril/mithril-node-go/internal/domain"
	"github.com/rs/zerolog"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/encoding/protojson"
)

type VolumeLabelPublisher struct {
	client *clientv3.Client
	logger *zerolog.Logger

	mu    sync.Mutex
	value *EventualValue
}

var _ domain.VolumeLabelPublisher = (*VolumeLabelPublisher)(nil)

func NewVolumeLabelPublisher(client *clientv3.Client, logger *zerolog.Logger) *VolumeLabelPublisher {
	return &VolumeLabelPublisher{
		client: client,
		logger: logger,
	}
}

func (p *VolumeLabelPublisher) Publish(node domain.NodeID, labels map[domain.VolumeID]map[string]string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.value == nil {
		key := fmt.Sprintf("/registry/nodes/%010d/labeled_volume_bitsets", uint32(node))

		p.value = NewEventualValue(p.client, key)
	}

	sets := map[string]map[string]*domain.VolumeSet{}
	for id, labels := range labels {
		for key, value := range labels {
			a, ok := sets[key]
			if !ok {
				a = map[string]*domain.VolumeSet{}
				sets[key] = a
			}

			b, ok := a[value]
			if !ok {
				b = domain.NewVolumeSet()
				a[value] = b
			}

			b.AddVolume(id)
		}
	}

	msg := nodev1.LabeledVolumeBitsets{
		NodeId: uint32(node),
	}
	for key, a := range sets {
		for value, set := range a {
			volumeIds, err := set.MarshalBinary()
			if err != nil {
				return
			}

			msg.Bitsets = append(msg.Bitsets, &nodev1.LabeledVolumeBitsets_LabeledVolumeBitset{
				Key:       key,
				Value:     value,
				VolumeIds: volumeIds,
			})
		}
	}

	value, err := protojson.Marshal(&msg)
	if err != nil {
		return
	}

	p.value.Set(string(value))
}
