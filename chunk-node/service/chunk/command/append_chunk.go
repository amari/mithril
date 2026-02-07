package command

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/errors"
	"github.com/amari/mithril/chunk-node/port/chunk"
	"github.com/amari/mithril/chunk-node/service/volume"
	"github.com/rs/zerolog"
)

type AppendChunkInput struct {
	WriteKey         []byte
	ExpectedVersion  uint64
	MinTailSlackSize int64
	Body             io.Reader
	BodySize         int64
}

type AppendChunkOutput struct {
	Chunk *domain.AvailableChunk
}

type AppendChunkHandler struct {
	Repo          chunk.ChunkRepository
	VolumeManager *volume.VolumeManager
	NowFunc       func() time.Time
}

func NewAppendChunkHandler(
	repo chunk.ChunkRepository,
	volumeManager *volume.VolumeManager,
) *AppendChunkHandler {
	return &AppendChunkHandler{
		Repo:          repo,
		VolumeManager: volumeManager,
		NowFunc:       time.Now,
	}
}

func (h *AppendChunkHandler) HandleAppendChunk(ctx context.Context, input *AppendChunkInput) (*AppendChunkOutput, error) {
	c, err := h.Repo.GetByWriterKey(ctx, input.WriteKey)
	if err != nil {
		return nil, err
	}

	availableChunk, ok := c.AsAvailable()
	if !ok {
		return nil, fmt.Errorf("%w: chunk with write key %x is not available", errors.ErrChunkWrongState, input.WriteKey)
	}

	if availableChunk.Version != input.ExpectedVersion {
		return nil, &errors.ChunkError{
			Cause: errors.ErrVersionMismatch,
			Chunk: &errors.Chunk{
				ID:      availableChunk.ID[:],
				Version: availableChunk.Version,
				Size:    availableChunk.Size,
			},
		}
	}

	vol, err := h.VolumeManager.GetVolumeByID(availableChunk.ID.VolumeID())
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("failed to get volume handle")

		return nil, &errors.ChunkError{
			Cause: err,
			Chunk: &errors.Chunk{
				ID:      availableChunk.ID[:],
				Version: availableChunk.Version,
				Size:    availableChunk.Size,
			},
		}
	}

	if err := vol.Chunks().AppendChunk(ctx, availableChunk.ID, availableChunk.Size, input.Body, input.BodySize, input.MinTailSlackSize); err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("failed to append chunk to volume")

		return nil, &errors.ChunkError{
			Cause: err,
			Chunk: &errors.Chunk{
				ID:      availableChunk.ID[:],
				Version: availableChunk.Version,
				Size:    availableChunk.Size,
			},
		}
	}

	newAvailableChunk := &domain.AvailableChunk{
		ID:        availableChunk.ID,
		WriterKey: availableChunk.WriterKey,
		Size:      availableChunk.Size + input.BodySize,
		Version:   availableChunk.Version + 1,
		CreatedAt: availableChunk.CreatedAt,
		UpdatedAt: h.NowFunc(),
	}

	if err := h.Repo.Store(ctx, newAvailableChunk); err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("failed to store updated available chunk")

		return nil, &errors.ChunkError{
			Cause: err,
			Chunk: &errors.Chunk{
				ID:      availableChunk.ID[:],
				Version: availableChunk.Version,
				Size:    availableChunk.Size,
			},
		}
	}

	return &AppendChunkOutput{
		Chunk: newAvailableChunk,
	}, nil
}
