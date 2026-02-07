package node

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/amari/mithril/chunk-node/port"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type EtcdNodeLabelPublisher struct {
	client                 *clientv3.Client
	nodeIdentityRepository port.NodeIdentityRepository
}

var _ port.NodeLabelPublisher = (*EtcdNodeLabelPublisher)(nil)

func NewNodeLabelPublisher(client *clientv3.Client, nodeIdentityRepository port.NodeIdentityRepository) port.NodeLabelPublisher {
	return &EtcdNodeLabelPublisher{
		client:                 client,
		nodeIdentityRepository: nodeIdentityRepository,
	}
}

func (a *EtcdNodeLabelPublisher) PublishLabels(ctx context.Context, labels map[string]string) error {
	nodeIdentity, err := a.nodeIdentityRepository.LoadNodeIdentity(ctx)
	if err != nil {
		return err
	}

	key := "/mithril/cluster/nodes/labels/" + fmt.Sprintf("%08x", nodeIdentity.NodeID)

	val, err := json.Marshal(labels)
	if err != nil {
		return err
	}

	_, err = a.client.KV.Put(ctx, key, string(val))
	if err != nil {
		return err
	}

	return nil
}
