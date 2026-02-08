package chunkerrors

import "github.com/amari/mithril/chunk-node/domain"

type ChunkError struct {
	err     error
	id      domain.ChunkID
	version uint64
	size    int64
}

func WithChunk(err error, id domain.ChunkID, version uint64, size int64) error {
	return &ChunkError{
		err:     err,
		id:      id,
		version: version,
		size:    size,
	}
}

func (e *ChunkError) Error() string {
	return e.err.Error()
}

func (e *ChunkError) Unwrap() error {
	return e.err
}

func (e *ChunkError) ChunkID() domain.ChunkID {
	return e.id
}

func (e *ChunkError) ChunkVersion() uint64 {
	return e.version
}

func (e *ChunkError) ChunkSize() int64 {
	return e.size
}
