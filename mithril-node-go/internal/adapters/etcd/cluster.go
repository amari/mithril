package adaptersetcd

import (
	"context"
	"strconv"
	"sync"
	"time"

	discoveryv1 "github.com/amari/mithril/gen/go/proto/mithril/cluster/discovery/v1"
	"github.com/amari/mithril/mithril-node-go/internal/domain"
	"github.com/cenkalti/backoff/v5"
	"github.com/rs/zerolog"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/encoding/protojson"
)

type ClusterMap struct {
	client *clientv3.Client
	logger *zerolog.Logger
	prefix Prefix

	mu        sync.RWMutex
	wg        sync.WaitGroup
	nodes     map[domain.NodeID]*domain.NodePresence
	ctx       context.Context
	cancelCtx context.CancelFunc
	subs      map[domain.NodeID]map[chan struct{}]struct{}
}

func NewClusterMap(client *clientv3.Client, logger *zerolog.Logger, prefix Prefix) *ClusterMap {
	return &ClusterMap{
		client: client,
		logger: logger,
		prefix: prefix,
		subs:   make(map[domain.NodeID]map[chan struct{}]struct{}),
	}
}

var (
	_ domain.NodePeerResolver = (*ClusterMap)(nil)
)

func (m *ClusterMap) Start(ctx context.Context) error {
	m.ctx, m.cancelCtx = context.WithCancel(context.Background())

	m.wg.Go(func() { m.watchLoop(m.ctx) })

	return nil
}

func (m *ClusterMap) Stop(ctx context.Context) error {
	if m.cancelCtx != nil {
		m.cancelCtx()
	}
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

func (m *ClusterMap) watchLoop(ctx context.Context) {
	bo := backoff.NewExponentialBackOff()
	bo.Reset()

	aliveNodesPrefix := m.prefix.DiscoveryNodePrefix()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		resp, err := m.client.KV.Get(ctx, aliveNodesPrefix, clientv3.WithPrefix())
		if err != nil {
			// TODO: log the error

			select {
			case <-ctx.Done():
				return
			case <-time.After(bo.NextBackOff()):
			}
			continue
		}
		bo.Reset()
		rev := resp.Header.Revision

		nodes := make(map[domain.NodeID]*domain.NodePresence, len(resp.Kvs))
		for _, kv := range resp.Kvs {
			var msg discoveryv1.NodeRecord
			if err := protojson.Unmarshal(kv.Value, &msg); err != nil {
				// TODO: log the error
				continue
			}

			nodeID := domain.NodeID(msg.GetNodeId())

			nodes[nodeID] = &domain.NodePresence{
				ID:    nodeID,
				Nonce: [32]byte(msg.GetNonce()),
				GRPC: domain.NodePresenceGRPC{
					URLs: msg.GetEndpoints().GetGrpc(),
				},
			}
		}
		m.mu.Lock()
		m.nodes = nodes
		m.mu.Unlock()

		m.logger.Debug().Int("count", len(nodes)).Int64("revision", rev).Msg("loaded nodes from etcd")

		ch := m.client.Watcher.Watch(ctx, aliveNodesPrefix, clientv3.WithPrefix(), clientv3.WithRev(rev))

	watchLoop:
		for {
			select {
			case <-ctx.Done():
				return
			case resp, ok := <-ch:
				if !ok {
					break watchLoop
				}
				if resp.Err() != nil {
					m.logger.Debug().Err(resp.Err()).Msg("watcher error")
					break watchLoop
				}
				bo.Reset()

				func() {
					m.mu.Lock()
					defer m.mu.Unlock()

					for _, event := range resp.Events {
						rev = event.Kv.ModRevision

						switch event.Type {
						case mvccpb.PUT:
							var msg discoveryv1.NodeRecord
							if err := protojson.Unmarshal(event.Kv.Value, &msg); err != nil {
								continue
							}

							nodeID := domain.NodeID(msg.GetNodeId())

							presence := &domain.NodePresence{
								ID:    nodeID,
								Nonce: [32]byte(msg.GetNonce()),
								GRPC: domain.NodePresenceGRPC{
									URLs: msg.GetEndpoints().GetGrpc(),
								},
							}

							oldPresence, ok := m.nodes[nodeID]
							if ok {
								if oldPresence.Equals(presence) {
									continue
								}
							}

							m.nodes[nodeID] = presence
							m.notifySubscribersNoLock(domain.NodeID(nodeID))
						case mvccpb.DELETE:
							rawNodeID := string(event.Kv.Key[len(aliveNodesPrefix):])
							nodeID, err := strconv.ParseUint(rawNodeID, 10, 32)
							if err != nil {
								continue
							}

							delete(m.nodes, domain.NodeID(nodeID))
							m.notifySubscribersNoLock(domain.NodeID(nodeID))
						}
					}
				}()
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(bo.NextBackOff()):
		}
	}
}

func (m *ClusterMap) Resolve(ctx context.Context, nodeID domain.NodeID) (*domain.NodePeer, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	node, ok := m.nodes[nodeID]
	if !ok {
		return nil, domain.ErrNodePeerNotFound
	}

	return &domain.NodePeer{
		ID: node.ID,
		GRPC: domain.NodePeerGRPC{
			URLs: node.GRPC.URLs,
		},
	}, nil
}

func (m *ClusterMap) Watch(ctx context.Context, nodeID domain.NodeID) (<-chan struct{}, error) {
	ch := make(chan struct{}, 1)

	m.mu.Lock()
	if m.subs[nodeID] == nil {
		m.subs[nodeID] = make(map[chan struct{}]struct{})
	}
	m.subs[nodeID][ch] = struct{}{}
	m.mu.Unlock()

	go func() {
		defer close(ch)

		select {
		case <-ctx.Done():
		case <-m.ctx.Done():
		}

		m.mu.Lock()
		defer m.mu.Unlock()
		if sub, ok := m.subs[nodeID]; ok {
			delete(sub, ch)
			if len(sub) == 0 {
				delete(m.subs, nodeID)
			}
		}
	}()

	return ch, nil
}

func (m *ClusterMap) notifySubscribers(nodeID domain.NodeID) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	m.notifySubscribersNoLock(nodeID)
}

func (m *ClusterMap) notifySubscribersNoLock(nodeID domain.NodeID) {
	subs, ok := m.subs[nodeID]
	if !ok {
		return
	}

	for ch := range subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}
