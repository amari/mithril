package adapternode

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/amari/mithril/chunk-node/domain"
	portnode "github.com/amari/mithril/chunk-node/port/node"
	"github.com/rs/zerolog"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const memberWatchGracePeriod = 5 * time.Minute

type subscriber struct {
	ch     chan portnode.ClusterMemberEvent
	ctx    context.Context
	cancel context.CancelFunc
}

type memberWatch struct {
	nodeID       domain.NodeID
	subscribers  map[*subscriber]struct{}
	watchCancel  context.CancelFunc
	idleTimer    *time.Timer
	lastActivity time.Time
}

// EtcdMemberWatchManager implements MemberWatchManager using etcd watches.
// Watches are shared across subscribers and stopped after 5 minutes of inactivity.
type EtcdMemberWatchManager struct {
	etcdClient *clientv3.Client
	log        *zerolog.Logger

	mu      sync.Mutex
	watches map[domain.NodeID]*memberWatch
}

var _ portnode.MemberWatchManager = (*EtcdMemberWatchManager)(nil)

// NewMemberWatchManager creates an etcd-backed member watch manager.
func NewMemberWatchManager(
	etcdClient *clientv3.Client,
	log *zerolog.Logger,
) portnode.MemberWatchManager {
	mgr := &EtcdMemberWatchManager{
		etcdClient: etcdClient,
		log:        log,
		watches:    make(map[domain.NodeID]*memberWatch),
	}

	// Start background reaper
	go mgr.reaper()

	return mgr
}

// Watch returns a channel that receives events for the specified node.
// The subscription remains active until the context is canceled.
func (m *EtcdMemberWatchManager) Watch(ctx context.Context, nodeID domain.NodeID) (<-chan portnode.ClusterMemberEvent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Get or create watch
	mw, exists := m.watches[nodeID]
	if !exists {
		mw = &memberWatch{
			nodeID:       nodeID,
			subscribers:  make(map[*subscriber]struct{}),
			lastActivity: time.Now(),
		}
		m.watches[nodeID] = mw

		// Start etcd watch
		if err := m.startWatch(mw); err != nil {
			delete(m.watches, nodeID)
			return nil, fmt.Errorf("failed to start watch: %w", err)
		}

		m.log.Debug().
			Uint32("node_id", uint32(nodeID)).
			Msg("started etcd watch for cluster member")
	} else {
		// Cancel idle timer if active
		if mw.idleTimer != nil {
			mw.idleTimer.Stop()
			mw.idleTimer = nil
		}
	}

	// Create subscriber
	ch := make(chan portnode.ClusterMemberEvent, 10)
	subCtx, cancel := context.WithCancel(ctx)
	sub := &subscriber{
		ch:     ch,
		ctx:    subCtx,
		cancel: cancel,
	}

	mw.subscribers[sub] = struct{}{}
	mw.lastActivity = time.Now()

	// Monitor context cancellation
	go m.monitorSubscriber(nodeID, sub)

	return ch, nil
}

func (m *EtcdMemberWatchManager) monitorSubscriber(nodeID domain.NodeID, sub *subscriber) {
	<-sub.ctx.Done()
	m.unsubscribe(nodeID, sub)
}

func (m *EtcdMemberWatchManager) unsubscribe(nodeID domain.NodeID, sub *subscriber) {
	m.mu.Lock()
	defer m.mu.Unlock()

	mw, exists := m.watches[nodeID]
	if !exists {
		return
	}

	// Remove subscriber
	delete(mw.subscribers, sub)
	close(sub.ch)

	// If no more subscribers, start idle timer
	if len(mw.subscribers) == 0 {
		mw.idleTimer = time.AfterFunc(memberWatchGracePeriod, func() {
			m.stopWatch(nodeID)
		})

		m.log.Debug().
			Uint32("node_id", uint32(nodeID)).
			Dur("grace_period", memberWatchGracePeriod).
			Msg("no subscribers remaining, starting idle timer")
	}
}

func (m *EtcdMemberWatchManager) startWatch(mw *memberWatch) error {
	ctx, cancel := context.WithCancel(context.Background())
	mw.watchCancel = cancel

	key := fmt.Sprintf("/mithril/cluster/presence/nodes/%08x", mw.nodeID)
	watchChan := m.etcdClient.Watch(ctx, key)

	// Fan-out goroutine
	go func() {
		for watchResp := range watchChan {
			if watchResp.Err() != nil {
				m.log.Error().
					Err(watchResp.Err()).
					Uint32("node_id", uint32(mw.nodeID)).
					Msg("etcd watch error")
				continue
			}

			for _, ev := range watchResp.Events {
				event := m.parseEvent(mw.nodeID, ev)
				if event == nil {
					continue
				}

				// Broadcast to all subscribers
				m.mu.Lock()
				for sub := range mw.subscribers {
					select {
					case sub.ch <- *event:
					case <-sub.ctx.Done():
						// Subscriber context canceled, skip
					default:
						// Subscriber slow, skip
						m.log.Warn().
							Uint32("node_id", uint32(mw.nodeID)).
							Msg("subscriber channel full, dropping event")
					}
				}
				mw.lastActivity = time.Now()
				m.mu.Unlock()
			}
		}
	}()

	return nil
}

func (m *EtcdMemberWatchManager) stopWatch(nodeID domain.NodeID) {
	m.mu.Lock()
	defer m.mu.Unlock()

	mw, exists := m.watches[nodeID]
	if !exists {
		return
	}

	// Only stop if still no subscribers and idle timer expired
	if len(mw.subscribers) > 0 {
		return
	}

	// Stop etcd watch
	if mw.watchCancel != nil {
		mw.watchCancel()
	}

	delete(m.watches, nodeID)

	m.log.Debug().
		Uint32("node_id", uint32(nodeID)).
		Msg("stopped etcd watch for cluster member")
}

func (m *EtcdMemberWatchManager) reaper() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		m.mu.Lock()
		for nodeID, mw := range m.watches {
			// Clean up watches with no subscribers that have been idle
			if len(mw.subscribers) == 0 && time.Since(mw.lastActivity) > memberWatchGracePeriod {
				if mw.watchCancel != nil {
					mw.watchCancel()
				}
				delete(m.watches, nodeID)

				m.log.Debug().
					Uint32("node_id", uint32(nodeID)).
					Msg("reaped idle member watch")
			}
		}
		m.mu.Unlock()
	}
}

func (m *EtcdMemberWatchManager) parseEvent(nodeID domain.NodeID, ev *clientv3.Event) *portnode.ClusterMemberEvent {
	switch ev.Type {
	case clientv3.EventTypePut:
		// Node announcement created or updated
		var announcement struct {
			StartupNonce string   `json:"startupNonce"`
			GRPCURLs     []string `json:"grpcURLs"`
		}

		if err := json.Unmarshal(ev.Kv.Value, &announcement); err != nil {
			m.log.Error().
				Err(err).
				Uint32("node_id", uint32(nodeID)).
				Msg("failed to unmarshal node announcement")
			return nil
		}

		startupNonce := uint64(0)
		if announcement.StartupNonce != "" {
			if _, err := fmt.Sscanf(announcement.StartupNonce, "%d", &startupNonce); err != nil {
				m.log.Error().
					Err(err).
					Uint32("node_id", uint32(nodeID)).
					Msg("failed to parse startup nonce")
				return nil
			}
		}

		member := &portnode.Member{
			NodeID:       nodeID,
			StartupNonce: startupNonce,
			GRPCURLs:     announcement.GRPCURLs,
		}

		// Determine if this is a new node or an update
		eventType := portnode.ClusterMemberEventTypeJoined
		if ev.Kv.Version > 1 {
			eventType = portnode.ClusterMemberEventTypeUpdated
		}

		return &portnode.ClusterMemberEvent{
			NodeID: nodeID,
			Type:   eventType,
			Node:   member,
		}

	case clientv3.EventTypeDelete:
		// Node left cluster
		return &portnode.ClusterMemberEvent{
			NodeID: nodeID,
			Type:   portnode.ClusterMemberEventTypeLeft,
			Node:   nil,
		}

	default:
		return nil
	}
}
