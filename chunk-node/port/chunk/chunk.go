package chunk

import (
	"context"
	"io"

	"github.com/amari/mithril/chunk-node/domain"
)

type Chunk interface {
	ID() domain.ChunkID

	Close() error

	NewReader(ctx context.Context) (ChunkReader, error)
	NewRangeReader(ctx context.Context, offset, length int64) (ChunkRangeReader, error)
	NewReaderAt(ctx context.Context) (ChunkReaderAt, error)
}

type ChunkReader interface {
	io.Reader
	io.Closer
}

type ChunkRangeReader interface {
	io.Reader
	io.Closer
}

type ChunkReaderAt interface {
	io.ReaderAt
	io.Closer
}
