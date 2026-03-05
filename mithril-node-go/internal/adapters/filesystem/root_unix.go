package adaptersfilesystem

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"

	adaptersunix "github.com/amari/mithril/mithril-node-go/internal/adapters/unix"
	"golang.org/x/sys/unix"
)

type FdSet struct {
	closeFds     []int
	fsyncFds     []int
	fdatasyncFds []int
}

func NewFdSet() *FdSet {
	return &FdSet{}
}

func (s *FdSet) Merge(other *FdSet) {
	if s == other {
		return
	}

	s.closeFds = append(s.closeFds, other.closeFds...)
	s.fsyncFds = append(s.fsyncFds, other.fsyncFds...)
	s.fdatasyncFds = append(s.fdatasyncFds, other.fdatasyncFds...)

	other.closeFds = other.closeFds[:0]
	other.fsyncFds = other.fsyncFds[:0]
	other.fdatasyncFds = other.fdatasyncFds[:0]
}

func (s *FdSet) AppendClose(fd int) {
	s.closeFds = append(s.closeFds, fd)
}

func (s *FdSet) AppendFsync(fd int) {
	s.fsyncFds = append(s.fsyncFds, fd)
}

func (s *FdSet) AppendFdatasync(fd int) {
	s.fdatasyncFds = append(s.fdatasyncFds, fd)
}

func (s *FdSet) Flush() error {
	var errs []error

	for _, fd := range slices.Backward(s.fdatasyncFds) {
		if err := adaptersunix.Fdatasync(fd); err != nil {
			errs = append(errs, err)
		}
	}

	for _, fd := range slices.Backward(s.fsyncFds) {
		if err := adaptersunix.Fsync(fd); err != nil {
			errs = append(errs, err)
		}
	}

	s.fdatasyncFds = s.fdatasyncFds[:0]
	s.fsyncFds = s.fsyncFds[:0]

	return errors.Join(errs...)
}

func (s *FdSet) Close() error {
	var errs []error

	if err := s.Flush(); err != nil {
		errs = append(errs, err)
	}

	for _, fd := range slices.Backward(s.closeFds) {
		if err := adaptersunix.Close(fd); err != nil {
			errs = append(errs, err)
		}
	}

	s.closeFds = s.closeFds[:0]

	return errors.Join(errs...)
}

type Root struct {
	*os.Root

	dirFd int
}

func OpenRoot(name string) (*Root, error) {
	undoFuncs := []func(){}
	defer func() {
		for _, undoFunc := range slices.Backward(undoFuncs) {
			undoFunc()
		}
	}()

	root, err := os.OpenRoot(name)
	if err != nil {
		return nil, err
	}
	undoFuncs = append(undoFuncs, func() {
		_ = root.Close()
	})

	dirFd, err := unix.Open(name, unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_RDONLY, 0o700)
	if err != nil {
		return nil, err
	}
	undoFuncs = append(undoFuncs, func() {
		_ = unix.Close(dirFd)
	})

	undoFuncs = nil

	return &Root{
		Root:  root,
		dirFd: dirFd,
	}, nil
}

func (r *Root) Close() error {
	var errs []error

	if err := adaptersunix.Close(r.dirFd); err != nil {
		errs = append(errs, err)
	}

	if err := r.Root.Close(); err != nil {
		errs = append(errs, err)
	}

	r.dirFd = 0

	return errors.Join(errs...)
}

func (r *Root) OpenRoot(name string) (*Root, error) {
	undoFuncs := []func(){}
	defer func() {
		for _, undoFunc := range slices.Backward(undoFuncs) {
			undoFunc()
		}
	}()

	root, err := r.Root.OpenRoot(name)
	if err != nil {
		return nil, err
	}
	undoFuncs = append(undoFuncs, func() {
		_ = root.Close()
	})

	dirFd, err := unix.Openat(r.dirFd, name, unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_RDONLY, 0o700)
	if err != nil {
		return nil, err
	}
	undoFuncs = append(undoFuncs, func() {
		_ = unix.Close(dirFd)
	})

	undoFuncs = nil

	return &Root{
		Root:  root,
		dirFd: dirFd,
	}, nil
}

func (r *Root) Mkdir(name string, perm os.FileMode) error {
	return r.MkdirWithFdSet(name, perm, nil)
}

func (r *Root) MkdirWithFdSet(name string, perm os.FileMode, fdSet *FdSet) error {
	err := unix.Mkdirat(r.dirFd, name, uint32(perm.Perm()))
	if err != nil {
		if errors.Is(err, unix.EEXIST) {
			return nil
		}

		return err
	}

	if fdSet != nil {
		fd, err := unix.Openat(r.dirFd, name, unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_RDONLY, 0)
		if err != nil {
			return err
		}

		fdSet.AppendFsync(r.dirFd)
		fdSet.AppendFsync(fd)
		fdSet.AppendClose(fd)
	}

	return nil
}

func (r *Root) MkdirAll(path string, perm os.FileMode) error {
	return r.MkdirAllWithFdSet(path, perm, nil)
}

func (r *Root) MkdirAllWithFdSet(path string, perm os.FileMode, fdSet *FdSet) error {
	dirFd := r.dirFd

	if fdSet != nil {
		fdSet.AppendFsync(r.dirFd)
	}

	// FIXME: handle leading `/`, and `..` components
	for name := range strings.SplitSeq(path, "/") {
		err := unix.Mkdirat(dirFd, name, uint32(perm.Perm()))
		if err != nil && !errors.Is(err, unix.EEXIST) {
			if errors.Is(err, unix.EEXIST) {
				return nil
			}

			// FIXME
			return fmt.Errorf("creating directory %q: %w", name, err)
		}

		fd, err := unix.Openat(dirFd, name, unix.O_DIRECTORY|unix.O_RDONLY, uint32(perm.Perm()))
		if err != nil {
			// FIXME
			return fmt.Errorf("opening directory %q: %w", name, err)
		}

		if fdSet != nil {
			fdSet.AppendFsync(fd)
			fdSet.AppendClose(fd)
		}

		dirFd = fd
	}

	return nil
}

func (r *Root) Fsync() error {
	return adaptersunix.Fsync(r.dirFd)
}
