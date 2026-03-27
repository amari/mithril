package adaptersetcd

import (
	"sort"
	"sync"

	registrynodev1 "github.com/amari/mithril/gen/go/proto/mithril/cluster/registry/node/v1"
	"github.com/amari/mithril/mithril-node-go/internal/domain"
	"github.com/rs/zerolog"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/encoding/protojson"
)

type VolumeLabelPublisher struct {
	client *clientv3.Client
	logger *zerolog.Logger
	prefix Prefix

	mu    sync.Mutex
	value *EventualValue
}

var _ domain.VolumeLabelPublisher = (*VolumeLabelPublisher)(nil)

func NewVolumeLabelPublisher(client *clientv3.Client, logger *zerolog.Logger, prefix Prefix) *VolumeLabelPublisher {
	return &VolumeLabelPublisher{
		client: client,
		logger: logger,
		prefix: prefix,
	}
}

func (p *VolumeLabelPublisher) Publish(node domain.NodeID, labels map[domain.VolumeID]map[string]string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.value == nil {
		key := p.prefix.RegistryNodeVolumesKey(node)

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

	msg := registrynodev1.VolumeRecord{
		NodeId:            uint32(node),
		LabeledVolumeSets: make([]*registrynodev1.VolumeRecord_LabelVolumeSet, 0, len(sets)),
	}
	for key, a := range sets {
		for value, set := range a {
			volumeIds, err := set.MarshalBinary()
			if err != nil {
				return
			}

			msg.LabeledVolumeSets = append(msg.LabeledVolumeSets, &registrynodev1.VolumeRecord_LabelVolumeSet{
				Label: &registrynodev1.Label{
					Key:   key,
					Value: value,
				},
				VolumeSet: volumeIds,
			})
		}
	}
	sort.Slice(msg.LabeledVolumeSets, func(i, j int) bool {
		return msg.LabeledVolumeSets[i].Label.Key < msg.LabeledVolumeSets[j].Label.Key
	})

	value, err := protojson.Marshal(&msg)
	if err != nil {
		return
	}

	p.value.Set(string(value))
}
