package adaptersfilesystem

import "errors"

var (
	ErrNodeClaimFileNotFound       = errors.New("node claim file not found")
	ErrNodeClaimFileReadFailed     = errors.New("failed to read node claim file")
	ErrNodeClaimFileDecodingFailed = errors.New("failed to decode node claim")
	ErrNodeClaimFileWriteFailed    = errors.New("failed to write node claim file")

	ErrNodeSeedFileNotFound       = errors.New("node seed file not found")
	ErrNodeSeedFileReadFailed     = errors.New("failed to read node seed file")
	ErrNodeSeedFileDecodingFailed = errors.New("failed to decode node seed")
	ErrNodeSeedFileWriteFailed    = errors.New("failed to write node seed file")
)
