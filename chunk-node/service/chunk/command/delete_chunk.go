package command

import (
	"context"
	"errors"
	"time"

	"github.com/amari/mithril/chunk-node/domain"
	chunkstoreerrors "github.com/amari/mithril/chunk-node/errors"
	"github.com/amari/mithril/chunk-node/port/chunk"
	"github.com/amari/mithril/chunk-node/service/volume"
)

type DeleteChunkInput struct {
	WriteKey []byte
}

type DeleteChunkOutput struct {
	Chunk *domain.DeletedChunk
}

type DeleteChunkHandler struct {
	Repo          chunk.ChunkRepository
	VolumeManager *volume.VolumeManager
	NowFunc       func() time.Time
}

func NewDeleteChunkHandler(
	repo chunk.ChunkRepository,
	volumeManager *volume.VolumeManager,
) *DeleteChunkHandler {
	return &DeleteChunkHandler{
		Repo:          repo,
		VolumeManager: volumeManager,
		NowFunc:       time.Now,
	}
}

func (h *DeleteChunkHandler) HandleDeleteChunk(ctx context.Context, input *DeleteChunkInput) (*DeleteChunkOutput, error) {
	c, err := h.Repo.GetByWriterKey(ctx, input.WriteKey)
	if err != nil {
		return nil, err
	}

	availableChunk, ok := c.AsAvailable()
	if !ok {
		return nil, chunkstoreerrors.ErrChunkWrongState
	}

	vol, err := h.VolumeManager.GetVolumeByID(availableChunk.ID.VolumeID())
	if err != nil {
		return nil, err
	}

	if err := vol.Chunks().DeleteChunk(ctx, availableChunk.ID); err != nil {
		if !errors.Is(err, chunkstoreerrors.ErrChunkNotFound) {
			return nil, err
		}
	}

	deletedChunk := &domain.DeletedChunk{
		ID:        availableChunk.ID,
		WriterKey: availableChunk.WriterKey,
		DeletedAt: h.NowFunc(),
	}

	if err := h.Repo.Store(ctx, deletedChunk); err != nil {
		if !errors.Is(err, chunkstoreerrors.ErrChunkNotFound) {
			return nil, err
		}
	}

	return &DeleteChunkOutput{
		Chunk: deletedChunk,
	}, nil
}
