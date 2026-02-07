package volume

import (
	"context"

	"github.com/amari/mithril/chunk-node/domain"
)

// DirectoryVolumeExpert provides low-level operations for directory-backed volumes.
// It handles formatting, reading metadata, and opening volumes stored as directories on disk.
type DirectoryVolumeExpert interface {
	// FormatDirectoryVolume formats a directory at the given path as a volume.
	// It writes the nodeID and volumeID to disk, creating the necessary volume metadata.
	// The nodeID is immutably set during formatting and cannot be changed later.
	//
	// Returns ErrVolumeAlreadyFormatted if the directory is already formatted as a volume.
	// Returns an error if the directory cannot be created or the format cannot be written.
	FormatDirectoryVolume(ctx context.Context, path string, nodeID domain.NodeID, volumeID domain.VolumeID) error

	// ReadDirectoryVolume reads the volume metadata from disk without validation.
	// It returns both the nodeID and volumeID that were written during formatting.
	// This method does not validate whether the nodeID matches the current node.
	//
	// Returns ErrVolumeNotFormatted if the directory is not formatted as a volume.
	// Returns ErrVolumeFormatInvalid if the volume metadata is malformed or corrupted.
	ReadDirectoryVolume(ctx context.Context, path string) (domain.NodeID, domain.VolumeID, error)

	// OpenDirectoryVolume opens a formatted directory volume for use.
	// It validates that the nodeID stored on disk matches the provided nodeID before opening.
	// This ensures that volumes formatted for one node are not accidentally opened on another node.
	//
	// Returns ErrVolumeNotFormatted if the directory is not formatted as a volume.
	// Returns ErrVolumeFormatInvalid if the nodeID does not match or the metadata is corrupted.
	// Returns a Volume interface that can be used to access the volume's chunks.
	OpenDirectoryVolume(ctx context.Context, path string, nodeID domain.NodeID) (Volume, error)
}
