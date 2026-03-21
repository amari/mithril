package applicationerrors

import (
	"errors"

	applicationservices "github.com/amari/mithril/mithril-node-go/internal/application/services"
	"github.com/amari/mithril/mithril-node-go/internal/domain"
)

type ContextError struct {
	err          error
	chunk        *domain.ReadyChunk
	volumeStatus *domain.VolumeStatus

	remotePeerNodeID       domain.NodeID
	remotePeerChunk        *applicationservices.RemotePeerChunk
	remotePeerVolumeStatus *domain.VolumeStatus
}

func NewContextError(err error) *ContextError {
	if contextErr, ok := err.(*ContextError); ok {
		// don't wrap when the error is already a ContextError

		return contextErr
	}

	if contextErr, ok := errors.AsType[*ContextError](err); ok {
		return &ContextError{
			err:                    err,
			chunk:                  contextErr.chunk,
			volumeStatus:           contextErr.volumeStatus,
			remotePeerNodeID:       contextErr.remotePeerNodeID,
			remotePeerChunk:        contextErr.remotePeerChunk,
			remotePeerVolumeStatus: contextErr.remotePeerVolumeStatus,
		}
	}

	return &ContextError{err: err}
}

func ContextErrorWithChunk(err error, c *domain.ReadyChunk) error {
	return NewContextError(err).WithChunk(c)
}

func ContextErrorWithVolumeStatus(err error, s domain.VolumeStatus) error {
	return NewContextError(err).WithVolumeStatus(&s)
}

func ContextErrorWithChunkAndVolumeStatus(err error, c *domain.ReadyChunk, s domain.VolumeStatus) error {
	return NewContextError(err).WithChunk(c).WithVolumeStatus(&s)
}

func ContextErrorWithRemotePeerNodeID(err error, nodeID domain.NodeID) error {
	return NewContextError(err).WithRemotePeerNodeID(nodeID)
}

func ContextErrorWithRemotePeerChunk(err error, c *applicationservices.RemotePeerChunk) error {
	return NewContextError(err).WithRemotePeerChunk(c)
}

func ContextErrorWithRemotePeerVolumeStatus(err error, s *domain.VolumeStatus) error {
	return NewContextError(err).WithRemotePeerVolumeStatus(s)
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

func (e *ContextError) RemotePeerNodeID() domain.NodeID {
	return e.remotePeerNodeID
}

func (e *ContextError) RemotePeerChunk() *applicationservices.RemotePeerChunk {
	return e.remotePeerChunk
}

func (e *ContextError) RemotePeerVolumeStatus() *domain.VolumeStatus {
	return e.remotePeerVolumeStatus
}

func (e *ContextError) WithChunk(c *domain.ReadyChunk) *ContextError {
	return &ContextError{err: e.err, chunk: c}
}

func (e *ContextError) WithVolumeStatus(s *domain.VolumeStatus) *ContextError {
	return &ContextError{err: e.err, volumeStatus: s}
}

func (e *ContextError) WithRemotePeerNodeID(nodeID domain.NodeID) *ContextError {
	return &ContextError{err: e.err, remotePeerNodeID: nodeID}
}

func (e *ContextError) WithRemotePeerChunk(c *applicationservices.RemotePeerChunk) *ContextError {
	return &ContextError{err: e.err, remotePeerChunk: c}
}

func (e *ContextError) WithRemotePeerVolumeStatus(s *domain.VolumeStatus) *ContextError {
	return &ContextError{err: e.err, remotePeerVolumeStatus: s}
}
