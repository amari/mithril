package volumeerrors

// RemoteStateError is an error that wraps another error and includes the state of the volume at the time of the error.
type RemoteStateError struct {
	err   error
	state State
}

// WithRemoteState wraps the given error with a VolumeRemoteStateError that includes the provided volume state.
func WithRemoteState(err error, state State) error {
	return &RemoteStateError{err: err, state: state}
}

// Error returns the error message of the wrapped error.
func (e *RemoteStateError) Error() string {
	return e.err.Error()
}

// Unwrap returns the wrapped error, allowing errors.Is and errors.As to work with VolumeRemoteStateError.
func (e *RemoteStateError) Unwrap() error {
	return e.err
}

// State returns the volume state associated with this error.
func (e *RemoteStateError) RemoteState() State {
	return e.state
}
