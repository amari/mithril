package chunkerrors

import "github.com/amari/mithril/chunk-node/domain"

type RemoteChunkError struct {
	err     error
	id      domain.ChunkID
	version uint64
	size    int64
}

func WithRemoteChunk(err error, id domain.ChunkID, version uint64, size int64) error {
	return &RemoteChunkError{
		err:     err,
		id:      id,
		version: version,
		size:    size,
	}
}

func (e *RemoteChunkError) Error() string {
	return e.err.Error()
}

func (e *RemoteChunkError) Unwrap() error {
	return e.err
}

func (e *RemoteChunkError) RemoteChunkID() domain.ChunkID {
	return e.id
}

func (e *RemoteChunkError) RemoteChunkVersion() uint64 {
	return e.version
}

func (e *RemoteChunkError) RemoteChunkSize() int64 {
	return e.size
}
