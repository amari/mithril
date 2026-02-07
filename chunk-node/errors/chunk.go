package errors

import "errors"

// Chunk Lifecycle & State Errors
var (
	// ErrChunkNotFound indicates a chunk lookup failed because it doesn't exist.
	// This can happen on read, append, delete, or stat operations.
	ErrChunkNotFound = errors.New("chunk not found")

	// ErrChunkWrongState indicates an operation is incompatible with the chunk's current state.
	// Examples: reading a TempChunk, appending to a DeletedChunk, putting data to an AvailableChunk.
	ErrChunkWrongState = errors.New("chunk wrong state")

	// ErrChunkExpired indicates a TempChunk has expired and should be garbage collected.
	ErrChunkExpired = errors.New("chunk expired")
)

// Authorization & Idempotency Errors
var (
	// ErrWriterKeyConflict indicates the WriterKey is already in use by a different chunk.
	// gRPC: ERROR_CODE_ALREADY_EXISTS
	// CSTP: ALREADY_EXISTS (10)
	ErrWriterKeyConflict = errors.New("writer key conflict")

	// ErrWriterKeyMismatch indicates the provided WriterKey doesn't match the chunk's WriterKey.
	// gRPC: codes.PermissionDenied + ERROR_CODE_WRITER_KEY_MISMATCH
	// CSTP: INVALID_ARGUMENT (3) with context
	ErrWriterKeyMismatch = errors.New("writer key mismatch")
)

// Concurrency Errors
var (
	// ErrVersionMismatch indicates optimistic concurrency control failed.
	// gRPC: ERROR_CODE_VERSION_MISMATCH
	// CSTP: VERSION_MISMATCH (2)
	ErrVersionMismatch = errors.New("version mismatch")
)

// Size & Bounds Errors
var (
	// ErrOffsetOutOfBounds indicates a read offset/length extends beyond the chunk's size.
	ErrOffsetOutOfBounds = errors.New("offset out of bounds")

	// ErrChunkTooLarge indicates the operation would cause the chunk to exceed size limits.
	ErrChunkTooLarge = errors.New("chunk too large")

	// ErrWriteSizeMismatch occurs when fewer bytes written than declared.
	// In CSTP this is SHORT_WRITE (8), but it's a transport error.
	// In gRPC it's codes.InvalidArgument + ERROR_CODE_INVALID_ARGUMENT
	ErrWriteSizeMismatch = errors.New("write size mismatch")
)

// ID Generation Errors
var (
	// ErrClockRegression indicates the system clock moved backwards.
	// The ID generator refuses to generate IDs to prevent duplicates.
	ErrClockRegression = errors.New("clock regression detected")

	// ErrSequenceOverflow indicates too many IDs were generated in the same millisecond.
	// This is extremely rare and indicates very high load.
	ErrSequenceOverflow = errors.New("sequence overflow")
)

// Identity Errors
var (
	// ErrWrongNode indicates the chunk belongs to a different node.
	ErrWrongNode = errors.New("wrong node")

	// ErrWrongVolume indicates the chunk belongs to a different volume.
	// This usually gets mapped to "Invalid Argument" in transport layers.
	ErrWrongVolume = errors.New("wrong volume")
)

type Chunk struct {
	ID      []byte
	Version uint64
	Size    int64
}
