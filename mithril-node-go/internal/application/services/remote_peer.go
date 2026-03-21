package applicationservices

import (
	"context"
	"io"

	"github.com/amari/mithril/mithril-node-go/internal/domain"
)

type RemotePeerChunkServiceClient interface {
	Stat(ctx context.Context, chunkID domain.ChunkID) (*RemotePeerChunkServiceStatResponse, error)
	OpenRangeReader(ctx context.Context, chunkID domain.ChunkID, offset int64, length int64) (RemotePeerChunkRangeReader, error)
}

type RemotePeerChunkServiceStatResponse struct {
	Chunk        *RemotePeerChunk
	VolumeStatus *domain.VolumeStatus
}

type RemotePeerChunkRangeReader interface {
	io.ReadCloser
	Chunk() *RemotePeerChunk
	VolumeStatus() *domain.VolumeStatus
}

type RemotePeerChunkServiceClientProvider interface {
	GetRemotePeerChunkServiceClient(nodeID domain.NodeID) (RemotePeerChunkServiceClient, error)
}

type RemotePeerChunk struct {
	id      domain.ChunkID
	size    int64
	version uint64
}

func NewRemotePeerChunk(id domain.ChunkID, size int64, version uint64) *RemotePeerChunk {
	return &RemotePeerChunk{
		id:      id,
		size:    size,
		version: version,
	}
}

func (c *RemotePeerChunk) ID() domain.ChunkID { return c.id }
func (c *RemotePeerChunk) Size() int64        { return c.size }
func (c *RemotePeerChunk) Version() uint64    { return c.version }
