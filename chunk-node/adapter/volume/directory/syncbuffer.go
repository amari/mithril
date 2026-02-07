package directory

import (
	"errors"
	"maps"

	"github.com/amari/mithril/chunk-node/unix"
)

type SyncBuffer struct {
	dataSyncFds []int
	syncFds     []int
	closeFds    map[int]struct{}
}

func NewSyncBuffer() *SyncBuffer {
	return &SyncBuffer{
		dataSyncFds: make([]int, 0),
		syncFds:     make([]int, 0),
		closeFds:    make(map[int]struct{}),
	}
}

func (sb *SyncBuffer) AddDataSyncFd(fd int) {
	sb.dataSyncFds = append(sb.dataSyncFds, fd)
}

func (sb *SyncBuffer) AddSyncFd(fd int) {
	sb.syncFds = append(sb.syncFds, fd)
}

func (sb *SyncBuffer) AddCloseFd(fd int) {
	sb.closeFds[fd] = struct{}{}
}

func (sb *SyncBuffer) Flush() error {
	var errs []error

	for _, fd := range sb.dataSyncFds {
		if err := unix.Fdatasync(fd); err != nil {
			errs = append(errs, err)
		}
	}

	for _, fd := range sb.syncFds {
		if err := unix.Fsync(fd); err != nil {
			errs = append(errs, err)
		}
	}

	sb.dataSyncFds = sb.dataSyncFds[:0]
	sb.syncFds = sb.syncFds[:0]

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}

func (sb *SyncBuffer) Close() error {
	var errs []error

	if err := sb.Flush(); err != nil {
		errs = append(errs, err)
	}

	for fd := range sb.closeFds {
		if err := unix.Close(fd); err != nil {
			errs = append(errs, err)
		}
	}

	sb.closeFds = make(map[int]struct{})

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}

func MergeSyncBuffers(dst *SyncBuffer, src *SyncBuffer) {
	if src == nil || dst == nil {
		return
	}

	maps.Copy(dst.closeFds, src.closeFds)
	dst.dataSyncFds = append(dst.dataSyncFds, src.dataSyncFds...)
	dst.syncFds = append(dst.syncFds, src.syncFds...)
}
