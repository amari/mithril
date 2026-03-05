package applicationerrors

import "github.com/amari/mithril/mithril-node-go/internal/domain"

type ContextError struct {
	err          error
	chunk        *domain.ReadyChunk
	volumeStatus *domain.VolumeStatus
}

func WithChunk(err error, c *domain.ReadyChunk) error {
	return &ContextError{err: err, chunk: c}
}

func WithVolumeStatus(err error, s domain.VolumeStatus) error {
	return &ContextError{err: err, volumeStatus: &s}
}

func WithChunkAndVolumeStatus(err error, c *domain.ReadyChunk, s domain.VolumeStatus) error {
	return &ContextError{err: err, chunk: c, volumeStatus: &s}
}

func (e *ContextError) Error() string {
	return e.err.Error()
}

func (e *ContextError) Unwrap() error {
	return e.err
}

func (e *ContextError) Chunk() *domain.ReadyChunk {
	return e.chunk
}

func (e *ContextError) VolumeStatus() *domain.VolumeStatus {
	return e.volumeStatus
}
