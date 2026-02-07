package etcd

import (
	"context"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v5"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Map struct {
	cli *clientv3.Client
	ttl int64

	mu      sync.RWMutex
	desired map[string]string
	leaseID clientv3.LeaseID

	started bool
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

func NewMap(cli *clientv3.Client, ttl int64) *Map {
	return &Map{
		cli:     cli,
		ttl:     ttl,
		desired: make(map[string]string),
	}
}

func (m *Map) Close() {
	if !m.started {
		return
	}
	m.cancel()
	m.wg.Wait()
	if m.leaseID != 0 {
		_, _ = m.cli.Revoke(context.Background(), m.leaseID)
	}
}

func (m *Map) ensureLease(ctx context.Context) (clientv3.LeaseID, error) {
	m.mu.RLock()
	if m.started && m.leaseID != 0 {
		id := m.leaseID
		m.mu.RUnlock()
		return id, nil
	}
	m.mu.RUnlock()

	m.mu.Lock()
	if m.started && m.leaseID != 0 {
		id := m.leaseID
		m.mu.Unlock()
		return id, nil
	}

	if !m.started {
		m.started = true
		m.ctx, m.cancel = context.WithCancel(context.Background())
		m.wg.Add(1)
		go m.keepaliveLoop()
	}
	m.mu.Unlock()

	var leaseResp *clientv3.LeaseGrantResponse

	op := func() (*clientv3.LeaseGrantResponse, error) {
		resp, err := m.cli.Grant(ctx, m.ttl)
		if err != nil {
			return nil, err
		}

		return resp, nil
	}

	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = time.Second
	bo.MaxInterval = 30 * time.Second
	bo.Multiplier = 2
	bo.RandomizationFactor = 0.5

	leaseResp, err := backoff.Retry(ctx, op, backoff.WithBackOff(bo))

	if err != nil {
		return 0, err
	}

	m.mu.Lock()
	m.leaseID = leaseResp.ID
	m.mu.Unlock()

	return leaseResp.ID, nil
}

func (m *Map) keepaliveLoop() {
	defer m.wg.Done()

	for {
		m.mu.RLock()
		id := m.leaseID
		ctx := m.ctx
		m.mu.RUnlock()

		if id == 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
				continue
			}
		}

		ch, err := m.cli.KeepAlive(ctx, id)
		if err != nil {
			m.mu.Lock()
			if m.leaseID == id {
				m.leaseID = 0
			}
			m.mu.Unlock()
			continue
		}

	loop:
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-ch:
				if !ok {
					m.mu.Lock()
					if m.leaseID == id {
						m.leaseID = 0
					}
					m.mu.Unlock()
					break loop
				}
			}
		}
	}
}

func (m *Map) Store(ctx context.Context, key, value string) error {
	leaseID, err := m.ensureLease(ctx)
	if err != nil {
		return err
	}

	if _, err := m.cli.Put(ctx, key, value, clientv3.WithLease(leaseID)); err != nil {
		return err
	}

	m.mu.Lock()
	m.desired[key] = value
	m.mu.Unlock()

	return nil
}

func (m *Map) Load(key string) (string, bool) {
	m.mu.RLock()
	v, ok := m.desired[key]
	m.mu.RUnlock()
	return v, ok
}

func (m *Map) LoadOrStore(ctx context.Context, key, value string) (string, bool, error) {
	m.mu.RLock()
	v, ok := m.desired[key]
	m.mu.RUnlock()
	if ok {
		return v, true, nil
	}

	if err := m.Store(ctx, key, value); err != nil {
		return "", false, err
	}

	return value, false, nil
}

func (m *Map) LoadAndDelete(ctx context.Context, key string) (string, bool, error) {
	m.mu.RLock()
	v, ok := m.desired[key]
	m.mu.RUnlock()
	if !ok {
		return "", false, nil
	}

	leaseID, err := m.ensureLease(ctx)
	if err != nil {
		return "", false, err
	}

	if _, err := m.cli.Delete(ctx, key, clientv3.WithLease(leaseID)); err != nil {
		return "", false, err
	}

	m.mu.Lock()
	delete(m.desired, key)
	m.mu.Unlock()

	return v, true, nil
}

func (m *Map) Delete(ctx context.Context, key string) error {
	_, ok := m.Load(key)
	if !ok {
		return nil
	}

	leaseID, err := m.ensureLease(ctx)
	if err != nil {
		return err
	}

	_ = leaseID

	if _, err := m.cli.Delete(ctx, key); err != nil {
		return err
	}

	m.mu.Lock()
	delete(m.desired, key)
	m.mu.Unlock()

	return nil
}

func (m *Map) Swap(ctx context.Context, key, value string) (string, bool, error) {
	m.mu.RLock()
	prev, ok := m.desired[key]
	m.mu.RUnlock()

	leaseID, err := m.ensureLease(ctx)
	if err != nil {
		return "", false, err
	}

	if _, err := m.cli.Put(ctx, key, value, clientv3.WithLease(leaseID)); err != nil {
		return "", false, err
	}

	m.mu.Lock()
	m.desired[key] = value
	m.mu.Unlock()

	return prev, ok, nil
}

func (m *Map) Clear(ctx context.Context) error {
	m.mu.RLock()
	snapshot := make(map[string]struct{}, len(m.desired))
	for k := range m.desired {
		snapshot[k] = struct{}{}
	}
	m.mu.RUnlock()

	if len(snapshot) == 0 {
		return nil
	}

	leaseID, err := m.ensureLease(ctx)
	if err != nil {
		return err
	}

	for k := range snapshot {
		if _, err := m.cli.Delete(ctx, k, clientv3.WithLease(leaseID)); err != nil {
			return err
		}
	}

	m.mu.Lock()
	m.desired = make(map[string]string)
	m.mu.Unlock()

	return nil
}
