package directory

import (
	"context"
	"io"
	"os"

	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/port"
)

type fileChunk struct {
	id   domain.ChunkID
	file *os.File
}

var _ port.Chunk = (*fileChunk)(nil)

func (fc *fileChunk) ID() domain.ChunkID {
	return fc.id
}

func (fc *fileChunk) NewReader(ctx context.Context) (port.ChunkReader, error) {
	return io.NopCloser(fc.file), nil
}

func (fc *fileChunk) NewRangeReader(ctx context.Context, offset, length int64) (port.ChunkRangeReader, error) {
	return io.NopCloser(io.NewSectionReader(fc.file, offset, length)), nil
}

func (fc *fileChunk) NewReaderAt(ctx context.Context) (port.ChunkReaderAt, error) {
	return &fileChunkReaderAt{
		File: fc.file,
	}, nil
}

func (fc *fileChunk) Close() error {
	return fc.file.Close()
}

type fileChunkReaderAt struct {
	*os.File
}

func (f *fileChunkReaderAt) Close() error {
	return nil
}
