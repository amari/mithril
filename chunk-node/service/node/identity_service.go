package node

import (
	"context"
	"errors"

	chunkstoreerrors "github.com/amari/mithril/chunk-node/errors"
	"github.com/amari/mithril/chunk-node/port"
	"github.com/rs/zerolog"
)

type NodeIdentityService struct {
	SeedGen  port.NodeSeedGenerator
	SeedRepo port.NodeSeedRepository
	Alloc    port.NodeIdentityAllocator
	Repo     port.NodeIdentityRepository
	Log      *zerolog.Logger
}

func NewNodeIdentityService(
	seedGen port.NodeSeedGenerator,
	seedRepo port.NodeSeedRepository,
	alloc port.NodeIdentityAllocator,
	repo port.NodeIdentityRepository,
	log *zerolog.Logger,
) *NodeIdentityService {
	return &NodeIdentityService{
		SeedGen:  seedGen,
		SeedRepo: seedRepo,
		Alloc:    alloc,
		Repo:     repo,
		Log:      log,
	}
}

func (s *NodeIdentityService) BootstrapNodeIdentity(ctx context.Context) error {
	s.Log.Info().Msg("Node identity loading...")

	nodeIdentity, err := s.Repo.LoadNodeIdentity(ctx)
	if err != nil {
		if !errors.Is(err, chunkstoreerrors.ErrNodeIdentityNotFound) {
			return err
		}
	}

	if nodeIdentity == nil {
		s.Log.Info().Msg("Node identity not found, creating new one...")

		seed, err := s.SeedRepo.LoadNodeSeed(ctx)
		if err != nil {
			if !errors.Is(err, chunkstoreerrors.ErrNodeSeedNotFound) {
				return err
			}
		}

		if seed == nil {
			seed, err = s.SeedGen.GenerateNodeSeed(ctx)
			if err != nil {
				return err
			}

			if err := s.SeedRepo.StoreNodeSeed(ctx, seed); err != nil {
				return err
			}
		}

		nodeIdentity, err = s.Alloc.AllocateNodeIdentity(ctx, seed)
		if err != nil {
			return err
		}

		if err := s.Repo.StoreNodeIdentity(ctx, nodeIdentity); err != nil {
			return err
		}

		s.Log.Info().Msg("Node identity created")
	} else {
		s.Log.Info().Msg("Node identity loaded")
	}

	return nil
}
