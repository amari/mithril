package adapternode

import (
	"context"
	"encoding/json"
	"errors"
	"os"

	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/nodeerrors"
	"github.com/amari/mithril/chunk-node/port"
)

type fileBackedNodeSeedRepository struct {
	path     string
	NodeSeed domain.NodeSeed
}

var _ port.NodeSeedRepository = (*fileBackedNodeSeedRepository)(nil)

func NewFileBackedNodeSeedRepository(path string) port.NodeSeedRepository {
	return &fileBackedNodeSeedRepository{
		path: path,
	}
}

func (r *fileBackedNodeSeedRepository) LoadNodeSeed(ctx context.Context) (domain.NodeSeed, error) {
	if r.NodeSeed != nil {
		return r.NodeSeed, nil
	}

	f, err := os.Open(r.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nodeerrors.ErrSeedNotFound
		}

		return nil, err
	}
	defer f.Close()

	var NodeSeed domain.NodeSeed

	err = json.NewDecoder(f).Decode(&NodeSeed)
	if err != nil {
		return nil, err
	}

	r.NodeSeed = NodeSeed

	return NodeSeed, nil
}

func (r *fileBackedNodeSeedRepository) StoreNodeSeed(ctx context.Context, seed domain.NodeSeed) error {
	f, err := os.OpenFile(r.path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()

	err = json.NewEncoder(f).Encode(seed)
	if err != nil {
		return err
	}

	r.NodeSeed = seed

	return nil
}
