package adaptersgrpcserver

import (
	chunkv1 "github.com/amari/mithril/gen/go/proto/mithril/chunk/v1"
	applicationservices "github.com/amari/mithril/mithril-node-go/internal/application/services"
	"github.com/amari/mithril/mithril-node-go/internal/domain"
)

func ChunkFromDomain(chunk *domain.ReadyChunk) *chunkv1.Chunk {
	if chunk == nil {
		return nil
	}

	version, _ := chunk.Version()
	size, _ := chunk.Size()

	return &chunkv1.Chunk{
		Id:      chunk.ID().Bytes(),
		Version: version,
		Size:    size,
	}
}

func VolumeFromDomain(status *domain.VolumeStatus) *chunkv1.Volume {
	if status == nil {
		return nil
	}

	state := chunkv1.Volume_STATE_UNSPECIFIED

	switch status.Health {
	case domain.VolumeOK:
		state = chunkv1.Volume_STATE_OK
	case domain.VolumeDegraded:
		state = chunkv1.Volume_STATE_DEGRADED
	case domain.VolumeFailed:
		state = chunkv1.Volume_STATE_FAILED
	case domain.VolumeUnknown:
		state = chunkv1.Volume_STATE_UNSPECIFIED
	}

	return &chunkv1.Volume{
		State: state,
	}
}

func RemotePeerChunkFromDomain(chunk *applicationservices.RemotePeerChunk) *chunkv1.Chunk {
	if chunk == nil {
		return nil
	}

	version := chunk.Version()
	size := chunk.Size()

	return &chunkv1.Chunk{
		Id:      chunk.ID().Bytes(),
		Version: version,
		Size:    size,
	}
}
