package volumeerrors

import "github.com/amari/mithril/chunk-node/domain"

type State = domain.VolumeState

const (
	StateUnknown  = domain.VolumeStateUnknown
	StateOK       = domain.VolumeStateOK
	StateDegraded = domain.VolumeStateDegraded
	StateFailed   = domain.VolumeStateFailed
)

// StateError is an error that wraps another error and includes the state of the volume at the time of the error.
type StateError struct {
	err   error
	state State
}

// WithState wraps the given error with a VolumeStateError that includes the provided volume state.
func WithState(err error, state State) error {
	return &StateError{err: err, state: state}
}

// Error returns the error message of the wrapped error.
func (e *StateError) Error() string {
	return e.err.Error()
}

// Unwrap returns the wrapped error, allowing errors.Is and errors.As to work with VolumeStateError.
func (e *StateError) Unwrap() error {
	return e.err
}

// State returns the volume state associated with this error.
func (e *StateError) State() State {
	return e.state
}
