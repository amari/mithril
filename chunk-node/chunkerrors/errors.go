package chunkerrors

import "errors"

// Sentinel errors
var (
	// ErrNotFound indicates a chunk lookup failed because it doesn't exist.
	// This can happen on read, append, delete, or stat operations.
	ErrNotFound = errors.New("chunk not found")
	// ErrWrongState indicates an operation is incompatible with the chunk's current state.
	// Examples: reading a TempChunk, appending to a DeletedChunk, putting data to an AvailableChunk.
	ErrWrongState = errors.New("chunk wrong state")
	// ErrExpired indicates a TempChunk has expired and should be garbage collected.
	ErrExpired = errors.New("chunk expired")
	// ErrWriterKeyConflict indicates the WriterKey is already in use by a different chunk.
	// gRPC: ERROR_CODE_ALREADY_EXISTS
	// CSTP: ALREADY_EXISTS (10)
	ErrWriterKeyConflict = errors.New("writer key conflict")
	// ErrWriterKeyMismatch indicates the provided WriterKey doesn't match the chunk's WriterKey.
	// gRPC: codes.PermissionDenied + ERROR_CODE_WRITER_KEY_MISMATCH
	// CSTP: INVALID_ARGUMENT (3) with context
	ErrWriterKeyMismatch = errors.New("writer key mismatch")
	// ErrVersionMismatch indicates optimistic concurrency control failed.
	// gRPC: ERROR_CODE_VERSION_MISMATCH
	// CSTP: VERSION_MISMATCH (2)
	ErrVersionMismatch = errors.New("version mismatch")
	// ErrOffsetOutOfBounds indicates a read offset/length extends beyond the chunk's size.
	ErrOffsetOutOfBounds = errors.New("offset out of bounds")
	// ErrTooLarge indicates the operation would cause the chunk to exceed size limits.
	ErrTooLarge = errors.New("chunk too large")
	// ErrWriteSizeMismatch occurs when fewer bytes written than declared.
	// In CSTP this is SHORT_WRITE (8), but it's a transport error.
	// In gRPC it's codes.InvalidArgument + ERROR_CODE_INVALID_ARGUMENT
	ErrWriteSizeMismatch = errors.New("write size mismatch")
	// ErrClockRegression indicates the system clock moved backwards.
	// The ID generator refuses to generate IDs to prevent duplicates.
	ErrClockRegression = errors.New("clock regression detected")
	// ErrSequenceOverflow indicates too many IDs were generated in the same millisecond.
	// This is extremely rare and indicates very high load.
	ErrSequenceOverflow = errors.New("sequence overflow")
	// ErrWrongNode indicates the chunk belongs to a different node.
	ErrWrongNode = errors.New("wrong node")
	// ErrWrongVolume indicates the chunk belongs to a different volume.
	// This usually gets mapped to "Invalid Argument" in transport layers.
	ErrWrongVolume = errors.New("wrong volume")
	// ErrAlreadyExists indicates a chunk with the same ID or WriterKey already exists.
	ErrAlreadyExists = errors.New("chunk already exists")
	// ErrCorrupted indicates the chunk's data is corrupted and cannot be read.
	ErrCorrupted = errors.New("chunk corrupted")
	// ErrInvalidID indicates the chunk ID is malformed or invalid.
	ErrInvalidID = errors.New("chunk invalid id")
)
