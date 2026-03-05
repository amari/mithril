package applicationservices

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/amari/mithril/mithril-node-go/internal/domain"
)

type NodeSeedService struct {
	NodeSeedGenerator  domain.NodeSeedGenerator
	NodeSeedRepository domain.NodeSeedRepository
}

func NewNodeSeedService(nodeSeedGenerator domain.NodeSeedGenerator, nodeSeedRepository domain.NodeSeedRepository) *NodeSeedService {
	return &NodeSeedService{
		NodeSeedGenerator:  nodeSeedGenerator,
		NodeSeedRepository: nodeSeedRepository,
	}
}

func (s *NodeSeedService) Start(ctx context.Context) error {
	seed, err := s.NodeSeedRepository.Get(ctx)
	if err == nil {
		return nil
	}

	if !errors.Is(err, domain.ErrNodeSeedNotFound) {
		return fmt.Errorf("%w: %w", ErrNodeSeedCheckFailed, err)
	}

	seed, err = s.NodeSeedGenerator.Generate()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrNodeSeedGenerationFailed, err)
	}

	if err = s.NodeSeedRepository.Upsert(ctx, seed); err != nil {
		return fmt.Errorf("%w: %w", ErrNodeSeedPersistFailed, err)
	}

	return nil
}

type NodeClaimService struct {
	NodeSeedRepository  domain.NodeSeedRepository
	NodeClaimRepository domain.NodeClaimRepository
	NodeClaimRegistry   domain.NodeClaimRegistry
}

func NewNodeClaimService(nodeSeedRepository domain.NodeSeedRepository, nodeClaimRepository domain.NodeClaimRepository, nodeClaimRegistry domain.NodeClaimRegistry) *NodeClaimService {
	return &NodeClaimService{
		NodeSeedRepository:  nodeSeedRepository,
		NodeClaimRepository: nodeClaimRepository,
		NodeClaimRegistry:   nodeClaimRegistry,
	}
}

func (s *NodeClaimService) Start(ctx context.Context) error {
	claim, err := s.NodeClaimRepository.Get(ctx)
	if err != nil {
		if !errors.Is(err, domain.ErrNodeClaimNotFound) {
			return fmt.Errorf("%w: %w", ErrNodeClaimLoadFailed, err)
		}

		// use seed to register a new claim
		seed, err := s.NodeSeedRepository.Get(ctx)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrNodeClaimSeedLoadFailed, err)
		}

		// Deterministically derive RNG seeds from the full node seed.
		sum := sha256.Sum256(seed[:])
		seed1 := binary.LittleEndian.Uint64(sum[0:8])
		seed2 := binary.LittleEndian.Uint64(sum[8:16])

		rng := rand.New(rand.NewPCG(seed1, seed2))

		// Deterministic per-seed proof (stable across restarts with the same seed).
		var proof [32]byte
		binary.LittleEndian.PutUint64(proof[:8], rng.Uint64())
		binary.LittleEndian.PutUint64(proof[8:16], rng.Uint64())
		binary.LittleEndian.PutUint64(proof[16:24], rng.Uint64())
		binary.LittleEndian.PutUint64(proof[24:32], rng.Uint64())

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			hasher := sha256.New()

			// Candidate-ID salt buffer (reused, no allocations).
			var saltBuf [16]byte

			// Next candidate ID from deterministic RNG stream.
			binary.LittleEndian.PutUint64(saltBuf[:8], rng.Uint64())
			binary.LittleEndian.PutUint64(saltBuf[8:], rng.Uint64())

			hasher.Write(seed)
			hasher.Write(saltBuf[:])

			idSum := hasher.Sum(nil)
			rawNodeID := binary.LittleEndian.Uint32(idSum[0:4])

			claim = &domain.NodeClaim{
				ID:    domain.NodeID(rawNodeID),
				Proof: proof,
			}

			err = s.NodeClaimRegistry.Register(ctx, claim)
			if err != nil {
				if errors.Is(err, domain.ErrNodeClaimConflict) {
					// try again with a new candidate ID
					continue
				} else {
					return fmt.Errorf("%w: %w", ErrNodeClaimRegisterFailed, err)
				}
			}

			if err := s.NodeClaimRepository.Upsert(ctx, claim); err != nil {
				return fmt.Errorf("%w: %w", ErrNodeClaimPersistFailed, err)
			}

			break
		}
	}

	// check the claim
	if err := s.NodeClaimRegistry.Check(ctx, claim); err != nil {
		return fmt.Errorf("%w: %w", ErrNodeClaimCheckFailed, err)
	}

	return nil
}

type NodeLabelService struct {
	sources    []domain.NodeLabelSource
	publisher  domain.NodeLabelPublisher
	idProvider domain.NodeIDProvider

	wg         sync.WaitGroup
	cancelFunc context.CancelFunc
	wakeCh     chan struct{}
}

func NewNodeLabelService(sources []domain.NodeLabelSource, publisher domain.NodeLabelPublisher, idProvider domain.NodeIDProvider) *NodeLabelService {
	return &NodeLabelService{
		sources:    sources,
		publisher:  publisher,
		idProvider: idProvider,
	}
}

func (s *NodeLabelService) Start(ctx context.Context) error {
	watchCtx, cancelFunc := context.WithCancel(context.Background())
	s.cancelFunc = cancelFunc
	s.wakeCh = make(chan struct{}, 1)

	for _, source := range s.sources {
		ch := source.Watch(watchCtx)

		s.wg.Go(func() {
			for {
				select {
				case <-watchCtx.Done():
				case _, ok := <-ch:
					select {
					case s.wakeCh <- struct{}{}:
					default:
					}

					if !ok {
						return
					}
				}
			}
		})
	}

	s.wg.Go(func() {
		s.scanAndPublish(watchCtx)
	})
	select {
	case s.wakeCh <- struct{}{}:
	default:
	}

	return nil
}

func (s *NodeLabelService) Stop() error {
	s.cancelFunc()

	s.wg.Wait()

	return nil
}

func (s *NodeLabelService) scanAndPublish(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.wakeCh:
		}

		labels := map[string]string{}

		for _, source := range s.sources {
			maps.Copy(labels, source.Read())
		}

		s.publisher.Publish(s.idProvider.GetNodeID(), labels)
	}
}

type NodePresenceService struct {
	NodeIDProvider        domain.NodeIDProvider
	NodePresencePublisher domain.NodePresencePublisher

	AdvertisedGRPCURLs NodeAdvertisedGRPCURLs
}

func NewNodePresenceService(nodeIDProvider domain.NodeIDProvider, nodePresencePublisher domain.NodePresencePublisher, advertisedGRPCURLs NodeAdvertisedGRPCURLs) *NodePresenceService {
	return &NodePresenceService{
		NodeIDProvider:        nodeIDProvider,
		NodePresencePublisher: nodePresencePublisher,
		AdvertisedGRPCURLs:    advertisedGRPCURLs,
	}
}

func (s *NodePresenceService) Start(ctx context.Context) error {
	now := uint64(time.Now().UnixNano())
	rng := rand.New(rand.NewPCG(now, now<<1|1))

	nonce := [32]byte{}
	binary.LittleEndian.PutUint64(nonce[:8], rng.Uint64())
	binary.LittleEndian.PutUint64(nonce[8:16], rng.Uint64())
	binary.LittleEndian.PutUint64(nonce[16:24], rng.Uint64())
	binary.LittleEndian.PutUint64(nonce[24:32], rng.Uint64())

	nodePresence := &domain.NodePresence{
		ID:    s.NodeIDProvider.GetNodeID(),
		Nonce: nonce,
		GRPC: domain.NodePresenceGRPC{
			URLs: s.AdvertisedGRPCURLs,
		},
	}

	go s.NodePresencePublisher.Publish(nodePresence)

	return nil
}

func (s *NodePresenceService) Stop(ctx context.Context) error {
	// no-op. Just a placeholder for now.
	return nil
}

type NodeAdvertisedGRPCURLs []string
