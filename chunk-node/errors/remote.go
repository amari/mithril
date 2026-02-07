package errors

import (
	chunkv1 "github.com/amari/mithril/gen/go/proto/mithril/chunk/v1"
)

// RemoteError wraps an error from a remote chunk node operation.
// Contains the remote node's ErrorDetails (chunk state, volume state, error code).
type RemoteError struct {
	Msg       string
	ErrorCode chunkv1.ErrorCode // From remote ErrorDetails

	Node   *Node
	Volume *Volume // Remote volume state (if provided)
	Chunk  *Chunk  // Remote chunk state (if provided)
}

func (e *RemoteError) Error() string {
	return e.Msg
}
