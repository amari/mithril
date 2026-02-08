package nodeerrors

import "github.com/amari/mithril/chunk-node/domain"

type RemoteNodeError struct {
	err error
	id  domain.NodeID
}

func WithRemoteNode(err error, id domain.NodeID) error {
	return &RemoteNodeError{
		err: err,
		id:  id,
	}
}

func (e *RemoteNodeError) Error() string {
	return e.err.Error()
}

func (e *RemoteNodeError) Unwrap() error {
	return e.err
}

func (e *RemoteNodeError) RemoteNodeID() domain.NodeID {
	return e.id
}
