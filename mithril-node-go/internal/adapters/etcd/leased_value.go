package adaptersetcd

import (
	"context"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v5"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// LeasedValue manages a single key-value pair in etcd with
// eventual consistency semantics. Writes are fire-and-forget and use CAS
// to avoid triggering etcd watchers when the value hasn't changed.
// If etcd is unavailable, writes are retried with exponential backoff.
// New writes supersede pending ones.
type LeasedValue struct {
	cli *clientv3.Client
	key string
	ttl int64

	mu      sync.RWMutex
	desired *string // nil = should be deleted, non-nil = desired value
	current bool    // true if etcd matches desired (as far as we know)

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// signals the sync goroutine that desired state changed
	notify chan struct{}
}

// NewLeasedValue creates a new LeasedValue for the given key.
func NewLeasedValue(cli *clientv3.Client, key string, ttl int64) *LeasedValue {
	ctx, cancel := context.WithCancel(context.Background())
	e := &LeasedValue{
		cli:     cli,
		key:     key,
		ttl:     ttl,
		current: true, // no desired state yet, so trivially current
		ctx:     ctx,
		cancel:  cancel,
		notify:  make(chan struct{}, 1),
	}
	e.wg.Add(1)
	go e.syncLoop()
	return e
}

// Set updates the desired value and triggers a background sync to etcd.
// This is fire-and-forget; it returns immediately.
// If a sync is already in progress, it will be superseded by this new value.
func (e *LeasedValue) Set(value string) {
	e.mu.Lock()
	e.desired = &value
	e.current = false
	e.mu.Unlock()

	e.signal()
}

// Delete marks the key for deletion from etcd.
// This is fire-and-forget; it returns immediately.
func (e *LeasedValue) Delete() {
	e.mu.Lock()
	e.desired = nil
	e.current = false
	e.mu.Unlock()

	e.signal()
}

// Get returns the local desired state.
// Returns (value, true) if a value is set, or ("", false) if deleted/unset.
// This is useful for debouncing - check if your new value differs before calling Set.
func (e *LeasedValue) Get() (string, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.desired == nil {
		return "", false
	}
	return *e.desired, true
}

// IsCurrent returns true if etcd is known to match the desired state.
// Returns false if a sync is pending or in progress.
func (e *LeasedValue) IsCurrent() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.current
}

// Close cancels any pending sync operations and waits for the sync goroutine to exit.
func (e *LeasedValue) Close() {
	e.cancel()
	e.wg.Wait()
}

func (e *LeasedValue) signal() {
	select {
	case e.notify <- struct{}{}:
	default:
		// already signaled
	}
}

func (e *LeasedValue) syncLoop() {
	defer e.wg.Done()

	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = 100 * time.Millisecond
	bo.MaxInterval = 10 * time.Second
	bo.Multiplier = 2
	bo.RandomizationFactor = 0.5

loop:
	for {
		select {
		case <-e.ctx.Done():
			return
		default:
		}

		resp, err := e.cli.Grant(e.ctx, e.ttl)
		if err != nil {
			// etcd error, backoff and retry
			wait := bo.NextBackOff()
			select {
			case <-e.ctx.Done():
				return
			case <-e.notify:
				// new desired state, restart with fresh backoff
				bo.Reset()
				continue loop
			case <-time.After(wait):
				continue loop
			}
		}

		leaseID := resp.ID

		keepAliveCh, err := e.cli.KeepAlive(e.ctx, leaseID)
		if err != nil {
			// etcd error, backoff and retry
			wait := bo.NextBackOff()
			select {
			case <-e.ctx.Done():
				return
			case <-e.notify:
				// new desired state, restart with fresh backoff
				bo.Reset()
				continue loop
			case <-time.After(wait):
				continue loop
			}
		}
		e.doSync(leaseID)

		bo.Reset()

		for {
			select {
			case <-e.ctx.Done():
				return
			case <-e.notify:
				e.doSync(leaseID)
			case _, ok := <-keepAliveCh:
				if !ok {
					continue loop
				}
				// lease is still alive, continue
			}
		}
	}
}

func (e *LeasedValue) doSync(leaseID clientv3.LeaseID) {
	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = 100 * time.Millisecond
	bo.MaxInterval = 10 * time.Second
	bo.Multiplier = 2
	bo.RandomizationFactor = 0.5

	for {
		e.mu.RLock()
		desired := e.desired
		e.mu.RUnlock()

		var synced bool
		var err error

		if desired == nil {
			synced, err = e.syncDelete()
		} else {
			synced, err = e.syncSet(*desired, leaseID)
		}

		if err != nil {
			// etcd error, backoff and retry
			wait := bo.NextBackOff()
			select {
			case <-e.ctx.Done():
				return
			case <-e.notify:
				// new desired state, restart with fresh backoff
				bo.Reset()
				continue
			case <-time.After(wait):
				continue
			}
		}

		if !synced {
			// CAS conflict, desired changed mid-sync, retry immediately
			continue
		}

		// success - check if desired changed while we were syncing
		e.mu.Lock()
		if e.desiredMatches(desired) {
			e.current = true
			e.mu.Unlock()
			return
		}
		e.mu.Unlock()
		// desired changed, loop again
	}
}

// desiredMatches checks if current desired state matches what we just synced.
// Must be called with mu held.
func (e *LeasedValue) desiredMatches(synced *string) bool {
	if e.desired == nil && synced == nil {
		return true
	}
	if e.desired == nil || synced == nil {
		return false
	}
	return *e.desired == *synced
}

// syncSet attempts to set the value in etcd using CAS to avoid unnecessary writes.
// Returns (true, nil) if synced successfully or value already matches.
// Returns (false, nil) if desired state changed during sync (caller should retry).
// Returns (false, err) on etcd error.
func (e *LeasedValue) syncSet(value string, leaseID clientv3.LeaseID) (bool, error) {
	// First, read current value from etcd
	resp, err := e.cli.Get(e.ctx, e.key)
	if err != nil {
		return false, err
	}

	// Check if desired changed while we were reading
	e.mu.RLock()
	if !e.desiredMatches(&value) {
		e.mu.RUnlock()
		return false, nil
	}
	e.mu.RUnlock()

	// If value already matches, we're done (no write, no watch trigger)
	if len(resp.Kvs) > 0 && string(resp.Kvs[0].Value) == value && resp.Kvs[0].Lease == int64(leaseID) {
		return true, nil
	}

	// Build CAS transaction
	var cmp clientv3.Cmp
	if len(resp.Kvs) > 0 {
		// Key exists, compare mod revision
		cmp = clientv3.Compare(clientv3.ModRevision(e.key), "=", resp.Kvs[0].ModRevision)
	} else {
		// Key doesn't exist
		cmp = clientv3.Compare(clientv3.CreateRevision(e.key), "=", 0)
	}

	txnResp, err := e.cli.Txn(e.ctx).
		If(cmp).
		Then(clientv3.OpPut(e.key, value, clientv3.WithLease(leaseID))).
		Commit()
	if err != nil {
		return false, err
	}

	if !txnResp.Succeeded {
		// CAS failed, someone else modified the key, retry
		return false, nil
	}

	return true, nil
}

// syncDelete attempts to delete the key from etcd using CAS.
// Returns (true, nil) if deleted or already doesn't exist.
// Returns (false, nil) if desired state changed during sync.
// Returns (false, err) on etcd error.
func (e *LeasedValue) syncDelete() (bool, error) {
	// First, check if key exists
	resp, err := e.cli.Get(e.ctx, e.key)
	if err != nil {
		return false, err
	}

	// Check if desired changed while we were reading
	e.mu.RLock()
	if !e.desiredMatches(nil) {
		e.mu.RUnlock()
		return false, nil
	}
	e.mu.RUnlock()

	// If key doesn't exist, we're done
	if len(resp.Kvs) == 0 {
		return true, nil
	}

	// CAS delete: only delete if mod revision matches what we read
	cmp := clientv3.Compare(clientv3.ModRevision(e.key), "=", resp.Kvs[0].ModRevision)

	txnResp, err := e.cli.Txn(e.ctx).
		If(cmp).
		Then(clientv3.OpDelete(e.key)).
		Commit()
	if err != nil {
		return false, err
	}

	if !txnResp.Succeeded {
		// CAS failed, retry
		return false, nil
	}

	return true, nil
}
