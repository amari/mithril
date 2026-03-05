package adapterspebble

import "errors"

var (
	// Generic adapter failure.
	ErrPebbleOperationFailed = errors.New("pebble operation failed")

	// Key not found in Pebble (maps to domain.ErrChunkNotFound in the repo).
	ErrPebbleNotFound = errors.New("pebble key not found")

	// JSON encoding/decoding failed.
	ErrPebbleModelEncodingFailed = errors.New("failed to encode chunk model")
	ErrPebbleModelDecodingFailed = errors.New("failed to decode chunk model")

	// Batch write failed.
	ErrPebbleBatchCommitFailed = errors.New("failed to commit pebble batch")

	// Repository startup/shutdown issues.
	ErrPebbleRepositoryStartFailed = errors.New("failed to start pebble repository")
	ErrPebbleRepositoryStopFailed  = errors.New("failed to stop pebble repository")

	ErrPebbleCounterOverflow = errors.New("pebble counter overflow")
)
