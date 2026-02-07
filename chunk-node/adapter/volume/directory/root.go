package directory

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

type Root struct {
	*os.Root

	dirfd int
}

func OpenRoot(path string) (*Root, error) {
	fd, err := unix.Open(path, unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}

	root, err := os.OpenRoot(path)
	if err != nil {
		_ = unix.Close(fd)
		return nil, err
	}

	return &Root{
		dirfd: fd,
		Root:  root,
	}, nil
}

func (r *Root) Close() error {
	err1 := r.Root.Close()
	err2 := unix.Close(r.dirfd)

	if err1 != nil {
		return err1
	}
	return err2
}

func (r *Root) Mkdir(name string, perm uint32) (*SyncBuffer, error) {
	fd, err := unix.Openat(r.dirfd, name, unix.O_DIRECTORY|unix.O_CREAT|unix.O_CLOEXEC|unix.O_RDONLY, perm)
	if err != nil {
		return nil, err
	}

	sb := NewSyncBuffer()
	sb.AddSyncFd(fd)
	sb.AddCloseFd(fd)

	return sb, nil
}

func (r *Root) MkdirAll(path string, perm uint32) (*SyncBuffer, error) {
	sb := NewSyncBuffer()

	dirfd := r.dirfd
	// FIXME: handle leading /
	// FIXME: handle non unix path separators
	for name := range strings.SplitSeq(path, "/") {
		err := unix.Mkdirat(dirfd, name, perm)
		if err != nil && !errors.Is(err, unix.EEXIST) {
			return nil, fmt.Errorf("creating directory %q: %w", name, err)
		}

		fd, err := unix.Openat(dirfd, name, unix.O_DIRECTORY|unix.O_RDONLY, perm)
		if err != nil {
			return nil, fmt.Errorf("opening directory %q: %w", name, err)
		}
		dirfd = fd

		sb.AddSyncFd(fd)
		sb.AddCloseFd(fd)
	}

	return sb, nil
}
