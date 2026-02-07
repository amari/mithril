package chunk

import "github.com/amari/mithril/chunk-node/domain"

// ChunkIDGenerator defines the interface for generating chunk IDs.
type ChunkIDGenerator interface {
	NextID(nodeID domain.NodeID, volumeID domain.VolumeID) (domain.ChunkID, error)
}
