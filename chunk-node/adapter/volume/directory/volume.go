package directory

import (
	"errors"
	"path/filepath"

	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/port"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
)

type directoryVolume struct {
	path       string
	volumeID   domain.VolumeID
	root       *Root
	chunkStore *directoryChunkStore
}

var _ portvolume.Volume = (*directoryVolume)(nil)

type DirectoryVolumeOptions struct {
	Path       string
	BufferSize int
}

func NewDirectoryVolume(volumeID domain.VolumeID, options DirectoryVolumeOptions) (portvolume.Volume, error) {
	root, err := OpenRoot(options.Path)
	if err != nil {
		return nil, err
	}

	chunkStoreRoot, err := OpenRoot(filepath.Join(options.Path, "chunks"))
	if err != nil {
		root.Close()
		return nil, err
	}

	chunkStore := NewChunkStore(chunkStoreRoot, options.BufferSize)

	return &directoryVolume{
		path:       options.Path,
		volumeID:   volumeID,
		root:       root,
		chunkStore: chunkStore,
	}, nil
}

func (dv *directoryVolume) ID() domain.VolumeID {
	return dv.volumeID
}

func (dv *directoryVolume) Close() error {
	var errs []error

	if err := dv.chunkStore.Close(); err != nil {
		errs = append(errs, err)
	}

	if err := dv.root.Close(); err != nil {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}

func (dv *directoryVolume) Chunks() port.ChunkStore {
	return dv.chunkStore
}
