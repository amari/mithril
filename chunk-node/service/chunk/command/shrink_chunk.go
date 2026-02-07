package command

import (
	"context"
	"fmt"
	"time"

	"github.com/amari/mithril/chunk-node/domain"
	chunkstoreerrors "github.com/amari/mithril/chunk-node/errors"
	"github.com/amari/mithril/chunk-node/port/chunk"
	"github.com/amari/mithril/chunk-node/service/volume"
	"github.com/rs/zerolog"
)

type ShrinkChunkInput struct {
	WriteKey         []byte
	ExpectedVersion  uint64
	MaxTailSlackSize int64
}

type ShrinkChunkOutput struct {
	Chunk *domain.AvailableChunk
}

type ShrinkChunkHandler struct {
	Repo          chunk.ChunkRepository
	VolumeManager *volume.VolumeManager
	NowFunc       func() time.Time
}

func NewShrinkChunkHandler(
	repo chunk.ChunkRepository,
	volumeManager *volume.VolumeManager,
) *ShrinkChunkHandler {
	return &ShrinkChunkHandler{
		Repo:          repo,
		VolumeManager: volumeManager,
		NowFunc:       time.Now,
	}
}

func (h *ShrinkChunkHandler) HandleShrinkChunk(ctx context.Context, input *ShrinkChunkInput) (*ShrinkChunkOutput, error) {
	chunk, err := h.Repo.GetByWriterKey(ctx, input.WriteKey)
	if err != nil {
		return nil, err
	}

	availableChunk, ok := chunk.AsAvailable()
	if !ok {
		return nil, fmt.Errorf("%w: chunk with write key %x is not available", chunkstoreerrors.ErrChunkWrongState, input.WriteKey)
	}

	if availableChunk.Version != input.ExpectedVersion {
		return nil, chunkstoreerrors.ErrVersionMismatch
	}

	vol, err := h.VolumeManager.GetVolumeByID(availableChunk.ID.VolumeID())
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("failed to get volume handle")

		return nil, err
	}

	if err := vol.Chunks().ShrinkChunkTailSlack(ctx, availableChunk.ID, availableChunk.Size, input.MaxTailSlackSize); err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("failed to shrink chunk tail slack")

		return nil, err
	}

	return &ShrinkChunkOutput{
		Chunk: availableChunk,
	}, nil
}
