package applicationservices

import "errors"

var (
	// ErrClockRegression indicates the system clock moved backwards.
	// The ID generator refuses to generate IDs to prevent duplicates.
	ErrClockRegression = errors.New("clock regression detected")

	// ErrChunkIDSequenceOverflow indicates too many IDs were generated in the same millisecond.
	// This is extremely rare and indicates very high load.
	ErrChunkIDSequenceOverflow = errors.New("chunk id sequence overflow")

	ErrNodeInitializationFailed = errors.New("node initialization failed")

	ErrNodeClaimRegistrationFailed = errors.New("failed to register node claim")
	ErrNodeClaimCheckFailed        = errors.New("failed to check node claim")
	ErrNodeClaimLoadFailed         = errors.New("failed to load node claim")
	ErrNodeClaimSeedLoadFailed     = errors.New("failed to load node seed")
	ErrNodeClaimRegisterFailed     = errors.New("failed to register node claim")
	ErrNodeClaimPersistFailed      = errors.New("failed to persist node claim")

	ErrNodeSeedCheckFailed      = errors.New("failed to check for existing node seed")
	ErrNodeSeedGenerationFailed = errors.New("failed to generate node seed")
	ErrNodeSeedMissing          = errors.New("node seed is missing")
	ErrNodeSeedPersistFailed    = errors.New("failed to persist node seed")

	ErrVolumeSelectionFailed = errors.New("volume selection failed")
	ErrUnknownVolume         = errors.New("unknown volume")

	ErrVolumeInitializationFailed = errors.New("volume initialization failed")
	ErrVolumeIDSequenceOverflow   = errors.New("volume id sequence overflow")
	ErrVolumeAlreadyOpen          = errors.New("volume already open")

	ErrVolumeStatFailed  = errors.New("volume stat failed")
	ErrVolumeOpenFailed  = errors.New("volume open failed")
	ErrVolumeCloseFailed = errors.New("volume close failed")
)
