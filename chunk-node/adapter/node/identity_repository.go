package node

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"

	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/nodeerrors"
	"github.com/amari/mithril/chunk-node/port"
)

type fileBackedNodeIdentityRepository struct {
	path string

	mu           sync.RWMutex
	nodeIdentity *domain.NodeIdentity
}

var _ port.NodeIdentityRepository = (*fileBackedNodeIdentityRepository)(nil)

func NewFileBackedNodeIdentityRepository(path string) port.NodeIdentityRepository {
	return &fileBackedNodeIdentityRepository{
		path: path,
	}
}

func (r *fileBackedNodeIdentityRepository) LoadNodeIdentity(ctx context.Context) (*domain.NodeIdentity, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.nodeIdentity != nil {
		return r.nodeIdentity, nil
	}

	f, err := os.Open(r.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nodeerrors.ErrIdentityNotFound
		}

		return nil, err
	}
	defer f.Close()

	var nodeIdentity domain.NodeIdentity

	err = json.NewDecoder(f).Decode(&nodeIdentity)
	if err != nil {
		return nil, err
	}

	r.nodeIdentity = &nodeIdentity

	return &nodeIdentity, nil
}

func (r *fileBackedNodeIdentityRepository) StoreNodeIdentity(ctx context.Context, identity *domain.NodeIdentity) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	f, err := os.OpenFile(r.path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()

	err = json.NewEncoder(f).Encode(identity)
	if err != nil {
		return err
	}

	r.nodeIdentity = identity

	return nil
}
