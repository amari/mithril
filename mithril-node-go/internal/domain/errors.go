package domain

import "errors"

var (
	// ERROR_CODE_INTERNAL (catch‑all when we can’t classify better)
	ErrInternal = errors.New("internal error")

	// ERROR_CODE_CANCELED
	ErrCanceled = errors.New("canceled")

	// ERROR_CODE_DEADLINE_EXCEEDED
	ErrDeadlineExceeded = errors.New("deadline exceeded")

	// ERROR_CODE_CHUNK_NOT_FOUND
	ErrChunkNotFound = errors.New("not found")

	// ERROR_CODE_CHUNK_ALREADY_EXISTS
	ErrChunkAlreadyExists = errors.New("already exists")

	// ERROR_CODE_CHUNK_CORRUPTED
	ErrChunkCorrupted = errors.New("chunk corrupted")

	// ERROR_CODE_CHUNK_WRITE_INCOMPLETE
	ErrChunkWriteIncomplete = errors.New("incomplete write")

	// ERROR_CODE_INVALID_OPERATION (e.g. deleted/pending when we expect ready)
	ErrChunkInvalidOperation = errors.New("invalid operation")

	// ERROR_CODE_CHUNK_INVALID_VERSION (for optimistic locking / append out-of-sync)
	ErrChunkInvalidVersion = errors.New("invalid version")

	// ERROR_CODE_CHUNK_INVALID_RANGE (bad offsets/lengths, negative sizes, etc.)
	ErrChunkInvalidRange = errors.New("invalid range")

	// ERROR_CODE_CHUNK_INVALID_ID (bad chunk ID format, etc.)
	ErrChunkInvalidID = errors.New("invalid id")

	// ERROR_CODE_CLOCK_NOT_MONOTONIC (clock moved backwards)
	ErrClockNotMonotonic = errors.New("non-monotonic clock")

	// ERROR_CODE_VOLUME_DEGRADED
	ErrVolumeDegraded = errors.New("volume degraded")

	// ERROR_CODE_VOLUME_FAILED
	ErrVolumeFailed = errors.New("volume failed")

	ErrNodeClaimNotFound = errors.New("node claim not found")
	ErrNodeClaimInvalid  = errors.New("node claim invalid")
	ErrNodeClaimConflict = errors.New("node claim conflict")

	ErrNodeSeedNotFound = errors.New("node seed not found")

	ErrVolumeUnavailable = errors.New("volume unavailable")

	ErrFileStoreVolumeAlreadyInitialized = errors.New("file store volume already initialized")
	ErrFileStoreVolumeNotInitialized     = errors.New("file store volume not initialized")
	ErrFileStoreVolumeCorrupt            = errors.New("file store volume corrupt")
	ErrFileStoreVolumeVersionMismatch    = errors.New("file store volume version mismatch")

	ErrNodePeerNotFound = errors.New("node peer not found")
)
