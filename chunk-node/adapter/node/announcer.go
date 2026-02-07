package node

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	infrastructureetcd "github.com/amari/mithril/chunk-node/adapter/infrastructure/etcd"
	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/port"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type EtcdNodeAnnouncer struct {
	map_                   *infrastructureetcd.Map
	nodeIdentityRepository port.NodeIdentityRepository
}

var _ port.NodeAnnouncer = (*EtcdNodeAnnouncer)(nil)

type etcdNodeAnnouncement struct {
	StartupNonce string   `json:"startupNonce"`
	GRPCURLs     []string `json:"grpcURLs"`
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

	etcdAnnouncement := etcdNodeAnnouncement{
		StartupNonce: strconv.FormatUint(announcement.StartupNonce, 10),
		GRPCURLs:     announcement.GRPCURLs,
	}

	value, err := json.Marshal(&etcdAnnouncement)
	if err != nil {
		return err
	}

	key := "/mithril/cluster/presence/nodes/" + fmt.Sprintf("%08x", nodeIdentity.NodeID)
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

	key := "/mithril/cluster/presence/nodes/" + fmt.Sprintf("%08x", nodeIdentity.NodeID)

	return a.map_.Delete(ctx, key)
}
