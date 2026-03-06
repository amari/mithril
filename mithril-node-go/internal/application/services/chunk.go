package applicationservices

import (
	"sync"
	"time"

	"github.com/amari/mithril/mithril-node-go/internal/domain"
)

type ChunkIDGenerator struct {
	nowFunc        func() time.Time
	nodeIDProvider domain.NodeIDProvider

	mu        sync.RWMutex
	timestamp int64
	sequence  int64
}

var _ domain.ChunkIDGenerator = (*ChunkIDGenerator)(nil)

func NewChunkIDGenerator(nowFunc func() time.Time, clockFence ClockFence, nodeIDProvider domain.NodeIDProvider) *ChunkIDGenerator {
	return &ChunkIDGenerator{
		nowFunc:        nowFunc,
		nodeIDProvider: nodeIDProvider,
		timestamp:      nowFunc().UnixMilli(),
	}
}

func (g *ChunkIDGenerator) Generate(volume domain.VolumeID) (domain.ChunkID, error) {
	now := g.nowFunc().UnixMilli()

	g.mu.Lock()
	defer g.mu.Unlock()

	if now < g.timestamp {
		return domain.ChunkID{}, &ClockRegressionError{
			CurrentTime: now,
			PrevTime:    g.timestamp,
		}
	}

	if now == g.timestamp {
		g.sequence++
		if g.sequence > int64(^uint16(0)) {
			return domain.ChunkID{}, ErrChunkIDSequenceOverflow
		}
	} else {
		g.timestamp = now
		g.sequence = 0
	}

	return domain.NewChunkID(now, g.nodeIDProvider.GetNodeID(), volume, uint16(g.sequence)), nil
}

type ChunkIDService struct {
	NodeIDProvider domain.NodeIDProvider
	NowFunc        func() time.Time

	mu       sync.RWMutex
	counters map[domain.VolumeID]*chunkIDCounter
}

func NewChunkIDService(nodeIDProvider domain.NodeIDProvider, _ ClockFence, nowFunc func() time.Time) *ChunkIDService {
	return &ChunkIDService{
		NodeIDProvider: nodeIDProvider,
		NowFunc:        nowFunc,
		counters:       make(map[domain.VolumeID]*chunkIDCounter),
	}
}

func (s *ChunkIDService) Next(volume domain.VolumeID) (domain.ChunkID, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	counter, ok := s.counters[volume]
	if !ok {
		return domain.ChunkID{}, ErrUnknownVolume
	}

	return counter.Next()
}

func (s *ChunkIDService) AddVolume(volume domain.VolumeID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.counters[volume]
	if !ok {
		s.counters[volume] = &chunkIDCounter{
			nowFunc: s.NowFunc,
			node:    s.NodeIDProvider.GetNodeID(),
			volume:  volume,
		}
	}
}

func (s *ChunkIDService) RemoveVolume(volume domain.VolumeID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.counters[volume]
	if ok {
		delete(s.counters, volume)
	}
}

type chunkIDCounter struct {
	nowFunc func() time.Time
	node    domain.NodeID
	volume  domain.VolumeID

	mu        sync.Mutex
	timestamp int64
	sequence  int64
}

func (g *chunkIDCounter) Next() (domain.ChunkID, error) {
	now := g.nowFunc().UnixMilli()

	g.mu.Lock()
	defer g.mu.Unlock()

	if now < g.timestamp {
		return domain.ChunkID{}, &ClockRegressionError{
			CurrentTime: now,
			PrevTime:    g.timestamp,
		}
	}

	if now == g.timestamp {
		g.sequence++
		if g.sequence > int64(^uint16(0)) {
			return domain.ChunkID{}, ErrChunkIDSequenceOverflow
		}
	} else {
		g.timestamp = now
		g.sequence = 0
	}

	return domain.NewChunkID(now, g.node, g.volume, uint16(g.sequence)), nil
}
