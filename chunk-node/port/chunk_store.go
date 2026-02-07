package port

import (
	"context"
	"io"

	"github.com/amari/mithril/chunk-node/domain"
)

// ChunkStore defines the interface for a chunk storage system.
type ChunkStore interface {
	ChunkExists(ctx context.Context, id domain.ChunkID) (bool, error)
	OpenChunk(ctx context.Context, id domain.ChunkID) (Chunk, error)
	CreateChunk(ctx context.Context, id domain.ChunkID, minTailSlackSize int64) error
	PutChunk(ctx context.Context, id domain.ChunkID, r io.Reader, n int64, minTailSlackSize int64) error
	AppendChunk(ctx context.Context, id domain.ChunkID, logicalSize int64, r io.Reader, n int64, minTailSlackSize int64) error
	DeleteChunk(ctx context.Context, id domain.ChunkID) error
	ShrinkChunkTailSlack(ctx context.Context, id domain.ChunkID, logicalSize int64, maxTailSlackSize int64) error
}
