package directory

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/amari/mithril/chunk-node/domain"
	chunkstoreerrors "github.com/amari/mithril/chunk-node/errors"
	"github.com/amari/mithril/chunk-node/port/volume"
)

type directoryVolumeExpert struct{}

var _ volume.DirectoryVolumeExpert = (*directoryVolumeExpert)(nil)

// NewDirectoryVolumeExpert creates a new instance of DirectoryVolumeExpert.
func NewDirectoryVolumeExpert() volume.DirectoryVolumeExpert {
	return &directoryVolumeExpert{}
}

func (d *directoryVolumeExpert) FormatDirectoryVolume(ctx context.Context, path string, nodeID domain.NodeID, volumeID domain.VolumeID) error {
	dirPath := filepath.Join(path, ".mithril.sys")

	formatJsonPath := filepath.Join(dirPath, "format.json")
	st, err := os.Stat(formatJsonPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	} else {
		if !st.Mode().IsRegular() {
			return chunkstoreerrors.ErrVolumeCorrupted
		}

		return chunkstoreerrors.ErrVolumeAlreadyFormatted
	}

	err = os.MkdirAll(dirPath, 0o755)
	if err != nil {
		return err
	}

	err = os.MkdirAll(filepath.Join(path, "chunks"), 0o755)
	if err != nil {
		return err
	}

	formatJsonTmpPath := filepath.Join(dirPath, "format.json.tmp")

	f, err := os.Create(formatJsonTmpPath)
	if err != nil {
		return err
	}
	defer f.Close()

	hostname, err := os.Hostname()
	if err != nil {
		return err
	}

	data, err := json.MarshalIndent(directoryVolumeFormat{
		Version:   1,
		NodeID:    uint32(nodeID),
		VolumeID:  uint16(volumeID),
		CreatedAt: time.Now(),
		Hostname:  hostname,
	}, "", "  ")

	if err != nil {
		return err
	}

	buf := bufio.NewWriter(f)

	_, err = io.Copy(buf, bytes.NewReader(data))
	if err != nil {
		return err
	}

	err = buf.Flush()
	if err != nil {
		return err
	}

	err = f.Sync()
	if err != nil {
		return err
	}

	err = os.Rename(formatJsonTmpPath, formatJsonPath)
	if err != nil {
		return err
	}

	return nil
}

func (d *directoryVolumeExpert) ReadDirectoryVolume(ctx context.Context, path string) (domain.NodeID, domain.VolumeID, error) {
	f, err := os.Open(filepath.Join(path, ".mithril.sys", "format.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return 0, 0, chunkstoreerrors.ErrVolumeNotFormatted
		}
		return 0, 0, err
	}
	defer f.Close()

	var format directoryVolumeFormat

	if err := json.NewDecoder(f).Decode(&format); err != nil {
		return 0, 0, chunkstoreerrors.ErrVolumeCorrupted
	}

	return domain.NodeID(format.NodeID), domain.VolumeID(format.VolumeID), nil
}

func (d *directoryVolumeExpert) OpenDirectoryVolume(ctx context.Context, path string, nodeID domain.NodeID) (volume.Volume, error) {
	f, err := os.Open(filepath.Join(path, ".mithril.sys", "format.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, chunkstoreerrors.ErrVolumeNotFormatted
		}
	}
	defer f.Close()

	var format directoryVolumeFormat

	if err := json.NewDecoder(f).Decode(&format); err != nil {
		return nil, chunkstoreerrors.ErrVolumeCorrupted
	}

	if domain.NodeID(format.NodeID) != nodeID {
		return nil, fmt.Errorf("%w: expected (%08x), actual (%08x)", chunkstoreerrors.ErrVolumeWrongNode, uint32(nodeID), format.NodeID)
	}

	return NewDirectoryVolume(domain.VolumeID(format.VolumeID), DirectoryVolumeOptions{
		Path: path,
	})
}

type directoryVolumeFormat struct {
	Version   uint32    `json:"version"`
	NodeID    uint32    `json:"nodeID"`
	VolumeID  uint16    `json:"volumeID"`
	CreatedAt time.Time `json:"createdAt"`
	Hostname  string    `json:"hostname"`
}
