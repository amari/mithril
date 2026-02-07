package query

import (
	"context"

	"github.com/amari/mithril/chunk-node/domain"
	chunkstoreerrors "github.com/amari/mithril/chunk-node/errors"
	"github.com/amari/mithril/chunk-node/port"
	"github.com/amari/mithril/chunk-node/port/chunk"
	"github.com/amari/mithril/chunk-node/port/volume"
)

type StatChunkInput struct {
	ChunkID   []byte
	WriterKey []byte
}

type StatChunkOutput struct {
	Chunk        *domain.AvailableChunk
	VolumeHealth *domain.VolumeHealth

	Handle port.Chunk
}

type StatChunkHandler struct {
	Repo                chunk.ChunkRepository
	VolumeHealthChecker volume.VolumeHealthChecker
}

func NewStatChunkHandler(
	repo chunk.ChunkRepository,
	volumeHealthChecker volume.VolumeHealthChecker,
) *StatChunkHandler {
	return &StatChunkHandler{
		Repo:                repo,
		VolumeHealthChecker: volumeHealthChecker,
	}
}

func (h *StatChunkHandler) HandleStatChunk(ctx context.Context, input *StatChunkInput) (*StatChunkOutput, error) {
	var (
		c   domain.Chunk
		err error
	)

	if len(input.ChunkID) > 0 {
		id, err := chunk.ChunkIDFromBytes(input.ChunkID)
		if err != nil {
			return nil, err
		}

		c, err = h.Repo.Get(ctx, id)
		if err != nil {
			return nil, err
		}
	} else if len(input.WriterKey) > 0 {
		c, err = h.Repo.GetByWriterKey(ctx, input.WriterKey)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, chunkstoreerrors.ErrWriterKeyMismatch
	}

	availableChunk, ok := c.AsAvailable()
	if !ok {
		return nil, chunkstoreerrors.ErrChunkWrongState
	}

	volumeHealth, err := h.VolumeHealthChecker.CheckVolumeHealth(availableChunk.ChunkID().VolumeID())
	if err != nil {
		return nil, err
	}

	return &StatChunkOutput{
		Chunk:        availableChunk,
		VolumeHealth: volumeHealth,
	}, nil
}
