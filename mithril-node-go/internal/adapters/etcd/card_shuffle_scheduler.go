package adaptersetcd

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sync"
	"time"

	nodev1 "github.com/amari/mithril/gen/go/proto/mithril/cluster/node/v1"
	cardshufflev1 "github.com/amari/mithril/gen/go/proto/mithril/cluster/scheduler/cardshuffle/v1"
	"github.com/amari/mithril/mithril-node-go/internal/domain"
	"github.com/cenkalti/backoff/v5"
	"github.com/rs/zerolog"
	clientv3 "go.etcd.io/etcd/client/v3"
	clientv3concurrency "go.etcd.io/etcd/client/v3/concurrency"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	electionPrefix = "/schedulers/card-shuffle/leader"
	generationKey  = "/schedulers/card-shuffle/generation"
	presencePrefix = "/alive/nodes/"
	decksPrefix    = "/schedulers/card-shuffle/decks"
)

type CardShuffleScheduler struct {
	client         *clientv3.Client
	nodeIDProvider domain.NodeIDProvider
	log            *zerolog.Logger

	// Configurable
	SessionTTL      int
	ShuffleInterval time.Duration

	generation    uint64
	lastDeckCount int

	wg                   sync.WaitGroup
	sessionCtx           context.Context
	sessionCtxCancelFunc context.CancelFunc
}

func NewCardShuffleScheduler(client *clientv3.Client, nodeIDProvider domain.NodeIDProvider, log *zerolog.Logger) *CardShuffleScheduler {
	if log == nil {
		l := zerolog.Nop()
		log = &l
	}

	return &CardShuffleScheduler{
		client:          client,
		nodeIDProvider:  nodeIDProvider,
		log:             log,
		SessionTTL:      10,
		ShuffleInterval: 5 * time.Second,
		generation:      uint64(rand.Uint32()),
		lastDeckCount:   -1,
	}
}

func (s *CardShuffleScheduler) Start(ctx context.Context) error {
	s.sessionCtx, s.sessionCtxCancelFunc = context.WithCancel(context.Background())
	nodeID := s.nodeIDProvider.GetNodeID()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.loop(s.sessionCtx, nodeID)
	}()

	return nil
}

func (s *CardShuffleScheduler) Stop(ctx context.Context) error {
	if s.sessionCtxCancelFunc != nil {
		s.sessionCtxCancelFunc()
	}
	s.wg.Wait()
	return nil
}

func (s *CardShuffleScheduler) loop(ctx context.Context, nodeID domain.NodeID) {
	bo := backoff.NewExponentialBackOff()
	bo.Reset()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		session, err := clientv3concurrency.NewSession(
			s.client,
			clientv3concurrency.WithTTL(s.SessionTTL),
			clientv3concurrency.WithContext(ctx),
		)
		if err != nil {
			wait := bo.NextBackOff()
			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
			}
			continue
		}

		bo.Reset()

		func() {
			defer session.Close()

			e := clientv3concurrency.NewElection(session, electionPrefix)

			if err := e.Campaign(ctx, fmt.Sprintf("%10d", nodeID)); err != nil {
				s.log.Debug().Err(err).Msg("campaign failed")
				return
			}

			// Count existing decks when becoming leader
			s.lastDeckCount = s.countDecksInEtcd(ctx)

			s.runLeaderLoop(ctx, session)

			_ = e.Resign(context.Background())
		}()
	}
}

func (s *CardShuffleScheduler) runLeaderLoop(ctx context.Context, session *clientv3concurrency.Session) {
	ticker := time.NewTicker(s.ShuffleInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-session.Done():
			return
		case <-ticker.C:
			s.performShuffleAndPublish(ctx)
		}
	}
}

func (s *CardShuffleScheduler) performShuffleAndPublish(ctx context.Context) {
	resp, err := s.client.Get(ctx, presencePrefix, clientv3.WithPrefix())
	if err != nil {
		s.log.Debug().Err(err).Msg("failed to get presences")
		return
	}

	nodeIDs := make([]uint32, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var p nodev1.Presence
		if err := protojson.Unmarshal(kv.Value, &p); err != nil {
			s.log.Debug().Err(err).Msg("failed to unmarshal presence")
			continue
		}
		nodeIDs = append(nodeIDs, p.NodeId)
	}

	if len(nodeIDs) == 0 {
		return
	}

	// Compute new deck count
	var newDeckCount int
	if len(nodeIDs) < 5 {
		newDeckCount = factorial(len(nodeIDs))
	} else {
		newDeckCount = 100
	}

	// Cleanup only when shrinking
	if s.lastDeckCount != -1 && newDeckCount < s.lastDeckCount {
		if _, err := s.client.Delete(ctx, decksPrefix, clientv3.WithPrefix()); err != nil {
			s.log.Debug().Err(err).Msg("failed to cleanup old decks")
		}
	}

	s.lastDeckCount = newDeckCount

	decks := s.generateUniqueDecks(nodeIDs, s.log)

	for i, deck := range decks {
		data, err := protojson.Marshal(deck)
		if err != nil {
			s.log.Debug().Err(err).Msg("failed to marshal deck")
			continue
		}

		key := fmt.Sprintf("%s/%03d", decksPrefix, i+1)
		if _, err := s.client.Put(ctx, key, string(data)); err != nil {
			s.log.Debug().Err(err).Str("key", key).Msg("failed to store deck")
		}
	}

	s.bumpGeneration(ctx)
}

func (s *CardShuffleScheduler) bumpGeneration(ctx context.Context) {
	s.generation += uint64(rand.Uint32())

	gen := &cardshufflev1.Generation{
		Generation: s.generation,
	}

	data, err := protojson.Marshal(gen)
	if err != nil {
		s.log.Debug().Err(err).Msg("failed to marshal generation")
		return
	}

	if _, err := s.client.Put(ctx, generationKey, string(data)); err != nil {
		s.log.Debug().Err(err).Msg("failed to store generation")
	}
}

func (s *CardShuffleScheduler) countDecksInEtcd(ctx context.Context) int {
	resp, err := s.client.Get(ctx, decksPrefix, clientv3.WithPrefix())
	if err != nil {
		s.log.Debug().Err(err).Msg("failed to count decks")
		return 0
	}
	return len(resp.Kvs)
}

func (s *CardShuffleScheduler) generateUniqueDecks(
	nodeIDs []uint32,
	log *zerolog.Logger,
) []*cardshufflev1.Deck {
	n := len(nodeIDs)

	var deckCount int
	if n < 5 {
		deckCount = factorial(n)
	} else {
		deckCount = 100
	}

	decks := make([]*cardshufflev1.Deck, 0, deckCount)
	seen := make(map[[32]byte]*cardshufflev1.Deck)

	hashDeck := func(d *cardshufflev1.Deck) [32]byte {
		h := sha256.New()
		buf := make([]byte, 4)

		for _, id := range d.NodeIds {
			binary.LittleEndian.PutUint32(buf, id)
			h.Write(buf)
		}

		var out [32]byte
		copy(out[:], h.Sum(nil))
		return out
	}

	if n < 5 {
		base := append([]uint32(nil), nodeIDs...)
		var permute func([]uint32, int)
		permute = func(a []uint32, i int) {
			if i == len(a) {
				d := &cardshufflev1.Deck{NodeIds: append([]uint32(nil), a...)}
				h := hashDeck(d)

				if existing, ok := seen[h]; ok {
					if !equalDecks(existing, d) {
						log.Error().
							Str("hash", fmt.Sprintf("%x", h)).
							Interface("existing_deck", existing.NodeIds).
							Interface("new_deck", d.NodeIds).
							Msg("SHA256 collision detected between different decks — catastrophic")
					}
					return
				}

				seen[h] = d
				decks = append(decks, d)
				return
			}
			for j := i; j < len(a); j++ {
				a[i], a[j] = a[j], a[i]
				permute(a, i+1)
				a[i], a[j] = a[j], a[i]
			}
		}
		permute(base, 0)

		rand.Shuffle(len(decks), func(i, j int) {
			decks[i], decks[j] = decks[j], decks[i]
		})

		return decks
	}

	for len(decks) < deckCount {
		cp := append([]uint32(nil), nodeIDs...)
		rand.Shuffle(len(cp), func(i, j int) {
			cp[i], cp[j] = cp[j], cp[i]
		})

		d := &cardshufflev1.Deck{NodeIds: cp}
		h := hashDeck(d)

		if existing, ok := seen[h]; ok {
			if !equalDecks(existing, d) {
				log.Error().
					Str("hash", fmt.Sprintf("%x", h)).
					Interface("existing_deck", existing.NodeIds).
					Interface("new_deck", d.NodeIds).
					Msg("SHA256 collision detected between different decks — catastrophic")
			}
			continue
		}

		seen[h] = d
		decks = append(decks, d)
	}

	return decks
}

func equalDecks(a, b *cardshufflev1.Deck) bool {
	if len(a.NodeIds) != len(b.NodeIds) {
		return false
	}
	for i := range a.NodeIds {
		if a.NodeIds[i] != b.NodeIds[i] {
			return false
		}
	}
	return true
}

func factorial(n int) int {
	r := 1
	for i := 2; i <= n; i++ {
		r *= i
	}
	return r
}
