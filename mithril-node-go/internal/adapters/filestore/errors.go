package adaptersfilestore

import "errors"

var (
	ErrFSOpenFailed      = errors.New("file store: open failed")
	ErrFSReadFailed      = errors.New("file store: read failed")
	ErrFSWriteFailed     = errors.New("file store: write failed")
	ErrFSStatFailed      = errors.New("file store: stat failed")
	ErrFSMkdirFailed     = errors.New("file store: mkdir failed")
	ErrFSRenameFailed    = errors.New("file store: rename failed")
	ErrFSFallocateFailed = errors.New("file store: fallocate failed")
	ErrFSFsyncFailed     = errors.New("file store: fsync failed")
	ErrFSFtruncateFailed = errors.New("file store: ftruncate failed")
	ErrFSCloseFailed     = errors.New("file store: close failed")
	ErrFSSeekFailed      = errors.New("file store: seek failed")
)
