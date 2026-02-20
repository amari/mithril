package etcd

import (
	"context"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v5"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// EventualValue manages a single key-value pair in etcd with
// eventual consistency semantics. Writes are fire-and-forget and use CAS
// to avoid triggering etcd watchers when the value hasn't changed.
// If etcd is unavailable, writes are retried with exponential backoff.
// New writes supersede pending ones.
type EventualValue struct {
	cli *clientv3.Client
	key string

	mu      sync.RWMutex
	desired *string // nil = should be deleted, non-nil = desired value
	current bool    // true if etcd matches desired (as far as we know)

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// signals the sync goroutine that desired state changed
	notify chan struct{}
}

// NewEventualValue creates a new EventualValue for the given key.
func NewEventualValue(cli *clientv3.Client, key string) *EventualValue {
	ctx, cancel := context.WithCancel(context.Background())
	e := &EventualValue{
		cli:     cli,
		key:     key,
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
func (e *EventualValue) Set(value string) {
	e.mu.Lock()
	e.desired = &value
	e.current = false
	e.mu.Unlock()

	e.signal()
}

// Delete marks the key for deletion from etcd.
// This is fire-and-forget; it returns immediately.
func (e *EventualValue) Delete() {
	e.mu.Lock()
	e.desired = nil
	e.current = false
	e.mu.Unlock()

	e.signal()
}

// Get returns the local desired state.
// Returns (value, true) if a value is set, or ("", false) if deleted/unset.
// This is useful for debouncing - check if your new value differs before calling Set.
func (e *EventualValue) Get() (string, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.desired == nil {
		return "", false
	}
	return *e.desired, true
}

// IsCurrent returns true if etcd is known to match the desired state.
// Returns false if a sync is pending or in progress.
func (e *EventualValue) IsCurrent() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.current
}

// Close cancels any pending sync operations and waits for the sync goroutine to exit.
func (e *EventualValue) Close() {
	e.cancel()
	e.wg.Wait()
}

func (e *EventualValue) signal() {
	select {
	case e.notify <- struct{}{}:
	default:
		// already signaled
	}
}

func (e *EventualValue) syncLoop() {
	defer e.wg.Done()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-e.notify:
			e.doSync()
		}
	}
}

func (e *EventualValue) doSync() {
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
			synced, err = e.syncSet(*desired)
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
func (e *EventualValue) desiredMatches(synced *string) bool {
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
func (e *EventualValue) syncSet(value string) (bool, error) {
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
	if len(resp.Kvs) > 0 && string(resp.Kvs[0].Value) == value {
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
		Then(clientv3.OpPut(e.key, value)).
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
func (e *EventualValue) syncDelete() (bool, error) {
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
