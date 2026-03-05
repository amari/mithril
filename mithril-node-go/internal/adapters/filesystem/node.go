package adaptersfilesystem

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/amari/mithril/mithril-node-go/internal/domain"
)

type NodeClaimModel struct {
	ID    domain.NodeID `json:"nodeID"`
	Proof [32]byte      `json:"proof"`
}

type NodeClaimRepository struct {
	path string

	mu    sync.RWMutex
	claim *domain.NodeClaim
}

var (
	_ domain.NodeClaimRepository = (*NodeClaimRepository)(nil)
	_ domain.NodeIDProvider      = (*NodeClaimRepository)(nil)
)

func NewNodeClaimRepository(path string) *NodeClaimRepository {
	return &NodeClaimRepository{
		path: path,
	}
}

func (r *NodeClaimRepository) Start(ctx context.Context) error {
	_, err := r.Get(ctx)
	if errors.Is(err, ErrNodeClaimFileNotFound) {
		return nil
	}

	return err
}

func (r *NodeClaimRepository) Get(ctx context.Context) (*domain.NodeClaim, error) {
	r.mu.RLock()
	if r.claim != nil {
		r.mu.RUnlock()
		return r.claim, nil
	}
	r.mu.RUnlock()

	r.mu.Lock()
	defer r.mu.Unlock()

	f, err := os.Open(r.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: %w", domain.ErrNodeClaimNotFound, ErrNodeClaimFileNotFound)
		}

		// Unexpected filesystem error.
		return nil, fmt.Errorf("%w: %w", ErrNodeClaimFileReadFailed, err)
	}
	defer f.Close()

	var model NodeClaimModel

	err = json.NewDecoder(f).Decode(&model)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrNodeClaimFileDecodingFailed, err)
	}

	claim := &domain.NodeClaim{
		ID:    model.ID,
		Proof: model.Proof,
	}
	r.claim = claim

	return claim, nil
}

func (r *NodeClaimRepository) Upsert(ctx context.Context, claim *domain.NodeClaim) error {
	r.mu.RLock()
	if r.claim != nil {
		r.mu.RUnlock()
		return nil
	}
	r.mu.RUnlock()

	r.mu.Lock()
	defer r.mu.Unlock()

	model := &NodeClaimModel{
		ID:    claim.ID,
		Proof: claim.Proof,
	}

	f, err := os.OpenFile(r.path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrNodeClaimFileWriteFailed, err)
	}
	defer f.Close()

	err = json.NewEncoder(f).Encode(model)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrNodeClaimFileWriteFailed, err)
	}

	r.claim = claim

	return nil
}

func (r *NodeClaimRepository) GetNodeID() domain.NodeID {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.claim == nil {
		return 0
	}

	return r.claim.ID
}

type NodeSeedRepository struct {
	path string

	mu   sync.RWMutex
	seed domain.NodeSeed
}

var _ domain.NodeSeedRepository = (*NodeSeedRepository)(nil)

func NewNodeSeedRepository(path string) *NodeSeedRepository {
	return &NodeSeedRepository{
		path: path,
	}
}

func (r *NodeSeedRepository) Start(ctx context.Context) error {
	_, err := r.Get(ctx)
	if errors.Is(err, ErrNodeSeedFileNotFound) {
		return nil
	}

	return err
}

func (r *NodeSeedRepository) Get(ctx context.Context) (domain.NodeSeed, error) {
	r.mu.RLock()
	if r.seed != nil {
		r.mu.RUnlock()
		return r.seed, nil
	}
	r.mu.RUnlock()

	r.mu.Lock()
	defer r.mu.Unlock()

	f, err := os.Open(r.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: %w", domain.ErrNodeSeedNotFound, ErrNodeSeedFileNotFound)
		}

		// Unexpected filesystem error.
		return nil, fmt.Errorf("%w: %w", ErrNodeSeedFileReadFailed, err)
	}
	defer f.Close()

	var seed domain.NodeSeed

	err = json.NewDecoder(f).Decode(&seed)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrNodeSeedFileDecodingFailed, err)
	}

	r.seed = seed

	return seed, nil
}

func (r *NodeSeedRepository) Upsert(ctx context.Context, seed domain.NodeSeed) error {
	r.mu.RLock()
	if r.seed != nil {
		r.mu.RUnlock()
		return nil
	}
	r.mu.RUnlock()

	r.mu.Lock()
	defer r.mu.Unlock()

	f, err := os.OpenFile(r.path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrNodeSeedFileWriteFailed, err)
	}
	defer f.Close()

	err = json.NewEncoder(f).Encode(seed)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrNodeSeedFileWriteFailed, err)
	}

	r.seed = seed

	return nil
}
