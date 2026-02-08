package command

import (
	"context"
	"fmt"
	"time"

	"github.com/amari/mithril/chunk-node/chunkerrors"
	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/port/chunk"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
	"github.com/amari/mithril/chunk-node/service/admission"
	"github.com/amari/mithril/chunk-node/service/volume"
	"github.com/rs/zerolog"
)

type ShrinkChunkInput struct {
	WriteKey         []byte
	ExpectedVersion  uint64
	MaxTailSlackSize int64
}

type ShrinkChunkOutput struct {
	Chunk        *domain.AvailableChunk
	VolumeHealth *domain.VolumeHealth
}

type ShrinkChunkHandler struct {
	Repo                chunk.ChunkRepository
	VolumeHealthChecker portvolume.VolumeHealthChecker
	VolumeManager       *volume.VolumeManager
	NowFunc             func() time.Time
}

func NewShrinkChunkHandler(
	repo chunk.ChunkRepository,
	volumeHealthChecker portvolume.VolumeHealthChecker,
	volumeManager *volume.VolumeManager,
) *ShrinkChunkHandler {
	return &ShrinkChunkHandler{
		Repo:                repo,
		VolumeHealthChecker: volumeHealthChecker,
		VolumeManager:       volumeManager,
		NowFunc:             time.Now,
	}
}

func (h *ShrinkChunkHandler) HandleShrinkChunk(ctx context.Context, input *ShrinkChunkInput) (*ShrinkChunkOutput, error) {
	chunk, err := h.Repo.GetByWriterKey(ctx, input.WriteKey)
	if err != nil {
		return nil, err
	}

	availableChunk, ok := chunk.AsAvailable()
	if !ok {
		return nil, fmt.Errorf("%w: chunk with write key %x is not available", chunkerrors.ErrWrongState, input.WriteKey)
	}

	if availableChunk.Version != input.ExpectedVersion {
		return nil, chunkerrors.ErrVersionMismatch
	}

	vol, err := h.VolumeManager.GetVolumeByID(availableChunk.ID.VolumeID())
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("failed to get volume handle")

		return nil, err
	}

	// check the volume health
	volumeHealth := h.VolumeHealthChecker.CheckVolumeHealth(availableChunk.ID.VolumeID())

	// perform admission control check before writing to the volume to avoid writing to a volume that is not healthy enough to accept writes
	if err := admission.AdmitWriteWithVolumeHealth(ctx, volumeHealth); err != nil {
		return nil, chunkerrors.WithChunk(
			err,
			availableChunk.ID,
			availableChunk.Version,
			availableChunk.Size,
		)
	}

	if err := vol.Chunks().ShrinkChunkTailSlack(ctx, availableChunk.ID, availableChunk.Size, input.MaxTailSlackSize); err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("failed to shrink chunk tail slack")

		return nil, err
	}

	return &ShrinkChunkOutput{
		Chunk:        availableChunk,
		VolumeHealth: h.VolumeHealthChecker.CheckVolumeHealth(availableChunk.ID.VolumeID()),
	}, nil
}
