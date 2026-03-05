package domain

import "errors"

var (
	// ERROR_CODE_NOT_FOUND
	ErrChunkNotFound = errors.New("chunk not found")

	// ERROR_CODE_ALREADY_EXISTS
	ErrChunkAlreadyExists = errors.New("chunk already exists")

	// ERROR_CODE_SHORT_WRITE
	ErrChunkShortWrite = errors.New("chunk short write")

	// ERROR_CODE_WRONG_STATE (e.g. deleted/pending when we expect ready)
	ErrChunkWrongState = errors.New("chunk in wrong state for operation")

	// ERROR_CODE_VERSION_MISMATCH (for optimistic locking / append out-of-sync)
	ErrChunkVersionMismatch = errors.New("chunk version / size mismatch")

	// ERROR_CODE_INVALID_ARGUMENT (bad offsets/lengths, negative sizes, etc.)
	ErrChunkInvalidArgument = errors.New("invalid chunk argument")

	// ERROR_CODE_NO_SPACE (ENOSPC / quota)
	ErrVolumeNoSpace = errors.New("volume has no space")

	// ERROR_CODE_VOLUME_DEGRADED / ERROR_CODE_VOLUME_FAILED
	ErrVolumeDegraded = errors.New("volume degraded")
	ErrVolumeFailed   = errors.New("volume failed")

	ErrChunkCorrupted = errors.New("chunk corrupted")

	// ERROR_CODE_INTERNAL (catch‑all when we can’t classify better)
	ErrChunkInternal = errors.New("chunk internal error")

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
