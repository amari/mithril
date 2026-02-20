package adapterchunk

import (
	"sync"
	"time"

	"github.com/amari/mithril/chunk-node/chunkerrors"
	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/port"
)

type chunkIDGenerator struct {
	mu        sync.Mutex
	timestamp int64
	sequence  int64
}

var _ port.ChunkIDGenerator = (*chunkIDGenerator)(nil)

func NewChunkIDGenerator() port.ChunkIDGenerator {
	return &chunkIDGenerator{}
}

func (g *chunkIDGenerator) NextID(nodeID domain.NodeID, volumeID domain.VolumeID) (domain.ChunkID, error) {
	now := time.Now().UnixMilli()

	g.mu.Lock()
	defer g.mu.Unlock()

	if now < g.timestamp {
		return domain.ChunkID{}, chunkerrors.ErrClockRegression
	}

	if now == g.timestamp {
		g.sequence++
		if g.sequence > int64(^uint16(0)) {
			return domain.ChunkID{}, chunkerrors.ErrSequenceOverflow
		}
	} else {
		g.timestamp = now
		g.sequence = 0
	}

	return domain.NewChunkID(now, uint32(nodeID), uint16(volumeID), uint16(g.sequence)), nil
}
