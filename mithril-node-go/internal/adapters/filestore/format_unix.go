//go:build unix
// +build unix

package adaptersfilestore

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	adaptersfilesystem "github.com/amari/mithril/mithril-node-go/internal/adapters/filesystem"
	"github.com/amari/mithril/mithril-node-go/internal/domain"
)

type FileStoreFormat struct{}

var _ domain.FileStoreFormat = (*FileStoreFormat)(nil)

func NewFileStoreFormat() *FileStoreFormat {
	return &FileStoreFormat{}
}

func (f *FileStoreFormat) Initialize(node domain.NodeID, volume domain.VolumeID, path string) error {
	root, err := adaptersfilesystem.OpenRoot(path)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrFSOpenFailed, err)
	}
	defer root.Close()

	_, err = f.statInRoot(root)
	if err != nil {
		if !errors.Is(err, domain.ErrFileStoreVolumeNotInitialized) {
			return err
		}
	} else {
		return domain.ErrFileStoreVolumeAlreadyInitialized
	}

	// not initialized
	_, err = root.Stat(".mithril.tmp")
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%w: %w", ErrFSStatFailed, err)
		}

		fdSet := adaptersfilesystem.NewFdSet()

		if err := root.MkdirWithFdSet(".mithril.tmp", 0o755, fdSet); err != nil {
			return fmt.Errorf("%w: %w", ErrFSMkdirFailed, err)
		}

		if err := fdSet.Flush(); err != nil {
			return fmt.Errorf("%w: %w", ErrFSFsyncFailed, err)
		}

		if err := fdSet.Close(); err != nil {
			return fmt.Errorf("%w: %w", ErrFSCloseFailed, err)
		}
	}

	innerRoot, err := root.OpenRoot(".mithril.tmp")
	if err != nil {
		return fmt.Errorf("%w: %w", ErrFSOpenFailed, err)
	}
	defer innerRoot.Close()

	if err := innerRoot.WriteFile("node-id", []byte(strconv.FormatUint(uint64(node), 10)), 0o644); err != nil {
		return fmt.Errorf("%w: %w", ErrFSWriteFailed, err)
	}

	if err := innerRoot.WriteFile("volume-id", []byte(strconv.FormatUint(uint64(volume), 10)), 0o644); err != nil {
		return fmt.Errorf("%w: %w", ErrFSWriteFailed, err)
	}

	if err := innerRoot.WriteFile("format-version", []byte(strconv.FormatUint(1, 10)), 0o644); err != nil {
		return fmt.Errorf("%w: %w", ErrFSWriteFailed, err)
	}

	if err := innerRoot.WriteFile("created-at", []byte(strconv.FormatInt(time.Now().UnixMilli(), 10)), 0o644); err != nil {
		return fmt.Errorf("%w: %w", ErrFSWriteFailed, err)
	}

	if err := root.Rename(".mithril.tmp", ".mithril"); err != nil {
		return fmt.Errorf("%w: %w", ErrFSRenameFailed, err)
	}

	if err := root.Fsync(); err != nil {
		return fmt.Errorf("%w: %w", ErrFSFsyncFailed, err)
	}

	return nil
}

func (f *FileStoreFormat) statInRoot(root *adaptersfilesystem.Root) (*domain.VolumeInfo, error) {
	root, err := root.OpenRoot(".mithril")
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: %w", domain.ErrFileStoreVolumeNotInitialized, err)
		}

		return nil, fmt.Errorf("%w: %w", ErrFSOpenFailed, err)
	}

	rawVersion, err := root.ReadFile("format-version")
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrFSReadFailed, err)
	}
	version, err := strconv.ParseUint(string(rawVersion), 10, 64)
	if err != nil {
		return nil, domain.ErrFileStoreVolumeCorrupt
	}
	if version != 1 {
		return nil, domain.ErrFileStoreVolumeVersionMismatch
	}

	rawNode, err := root.ReadFile("node-id")
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrFSReadFailed, err)
	}
	node, err := strconv.ParseUint(string(rawNode), 10, 32)
	if err != nil {
		return nil, domain.ErrFileStoreVolumeCorrupt
	}

	rawVolume, err := root.ReadFile("volume-id")
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrFSReadFailed, err)
	}
	volume, err := strconv.ParseUint(string(rawVolume), 10, 16)
	if err != nil {
		return nil, domain.ErrFileStoreVolumeCorrupt
	}

	return &domain.VolumeInfo{
		ID:   domain.VolumeID(volume),
		Node: domain.NodeID(node),
	}, nil
}

func (f *FileStoreFormat) Stat(path string) (*domain.VolumeInfo, error) {
	root, err := adaptersfilesystem.OpenRoot(path)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrFSOpenFailed, err)
	}
	defer root.Close()

	return f.statInRoot(root)
}

func (f *FileStoreFormat) Open(path string) (domain.VolumeHandle, error) {
	st, err := f.Stat(path)
	if err != nil {
		return nil, err
	}

	h := NewVolumeHandle(st.Node, st.ID, path)

	if err := h.start(); err != nil {
		return nil, err
	}

	return h, nil
}
