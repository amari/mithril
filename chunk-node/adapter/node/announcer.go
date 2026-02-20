package adapternode

import (
	"context"
	"encoding/binary"
	"fmt"

	infrastructureetcd "github.com/amari/mithril/chunk-node/adapter/infrastructure/etcd"
	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/port"
	nodev1 "github.com/amari/mithril/gen/go/proto/mithril/cluster/node/v1"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/encoding/protojson"
)

type EtcdNodeAnnouncer struct {
	map_                   *infrastructureetcd.Map
	nodeIdentityRepository port.NodeIdentityRepository
}

var _ port.NodeAnnouncer = (*EtcdNodeAnnouncer)(nil)

type etcdNodeAnnouncement struct {
	NodeID uint32 `json:"nodeID"`
	Nonce  string `json:"nonce"`
	GRPC   struct {
		URLs []string `json:"urls"`
	} `json:"grpc"`
}

func NewNodeAnnouncer(client *clientv3.Client, nodeIdentityRepository port.NodeIdentityRepository) port.NodeAnnouncer {
	m := infrastructureetcd.NewMap(client, 15)

	return &EtcdNodeAnnouncer{
		map_:                   m,
		nodeIdentityRepository: nodeIdentityRepository,
	}
}

func (a *EtcdNodeAnnouncer) SetAnnouncement(ctx context.Context, announcement *domain.NodeAnnouncement) error {
	nodeIdentity, err := a.nodeIdentityRepository.LoadNodeIdentity(ctx)
	if err != nil {
		return err
	}

	m := nodev1.Presence{
		NodeId: uint32(nodeIdentity.NodeID),
		Nonce:  binary.LittleEndian.AppendUint64(nil, announcement.StartupNonce),
		Grpc: &nodev1.Presence_GRPCInfo{
			Urls: announcement.GRPCURLs,
		},
	}

	value, err := protojson.Marshal(&m)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("/live/nodes/%08x", nodeIdentity.NodeID)
	if err := a.map_.Store(ctx, key, string(value)); err != nil {
		return err
	}

	return nil
}

func (a *EtcdNodeAnnouncer) ClearAnnouncement(ctx context.Context) error {
	nodeIdentity, err := a.nodeIdentityRepository.LoadNodeIdentity(ctx)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("/live/nodes/%08x", nodeIdentity.NodeID)

	return a.map_.Delete(ctx, key)
}
