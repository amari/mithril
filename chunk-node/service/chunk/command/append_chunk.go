package command

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/amari/mithril/chunk-node/chunkerrors"
	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/port/chunk"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
	"github.com/amari/mithril/chunk-node/service/admission"
	"github.com/amari/mithril/chunk-node/service/volume"
	"github.com/amari/mithril/chunk-node/volumeerrors"
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
	Chunk        *domain.AvailableChunk
	VolumeHealth *domain.VolumeHealth
}

type AppendChunkHandler struct {
	Repo                chunk.ChunkRepository
	VolumeHealthChecker portvolume.VolumeHealthChecker
	VolumeManager       *volume.VolumeManager
	NowFunc             func() time.Time
}

func NewAppendChunkHandler(
	repo chunk.ChunkRepository,
	volumeHealthChecker portvolume.VolumeHealthChecker,
	volumeManager *volume.VolumeManager,
) *AppendChunkHandler {
	return &AppendChunkHandler{
		Repo:                repo,
		VolumeHealthChecker: volumeHealthChecker,
		VolumeManager:       volumeManager,
		NowFunc:             time.Now,
	}
}

func (h *AppendChunkHandler) HandleAppendChunk(ctx context.Context, input *AppendChunkInput) (*AppendChunkOutput, error) {
	c, err := h.Repo.GetByWriterKey(ctx, input.WriteKey)
	if err != nil {
		return nil, err
	}

	availableChunk, ok := c.AsAvailable()
	if !ok {
		return nil, fmt.Errorf("%w: chunk with write key %x is not available", chunkerrors.ErrWrongState, input.WriteKey)
	}

	if availableChunk.Version != input.ExpectedVersion {
		return nil, chunkerrors.WithChunk(
			volumeerrors.WithState(chunkerrors.ErrVersionMismatch, h.VolumeHealthChecker.CheckVolumeHealth(availableChunk.ID.VolumeID()).State),
			availableChunk.ID,
			availableChunk.Version,
			availableChunk.Size,
		)
	}

	vol, err := h.VolumeManager.GetVolumeByID(availableChunk.ID.VolumeID())
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("failed to get volume handle")

		return nil, chunkerrors.WithChunk(
			err,
			availableChunk.ID,
			availableChunk.Version,
			availableChunk.Size,
		)
	}

	// check the volume health
	volumeHealth := h.VolumeHealthChecker.CheckVolumeHealth(availableChunk.ChunkID().VolumeID())

	// perform admission control check before writing to the volume to avoid writing to a volume that is not healthy enough to accept writes
	if err := admission.AdmitWriteWithVolumeHealth(ctx, volumeHealth); err != nil {
		return nil, chunkerrors.WithChunk(
			err,
			availableChunk.ID,
			availableChunk.Version,
			availableChunk.Size,
		)
	}

	if err := vol.Chunks().AppendChunk(ctx, availableChunk.ID, availableChunk.Size, input.Body, input.BodySize, input.MinTailSlackSize); err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("failed to append chunk to volume")

		return nil, chunkerrors.WithChunk(
			volumeerrors.WithState(
				err, h.VolumeHealthChecker.CheckVolumeHealth(availableChunk.ID.VolumeID()).State,
			),
			availableChunk.ID,
			availableChunk.Version,
			availableChunk.Size,
		)
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

		return nil, chunkerrors.WithChunk(
			volumeerrors.WithState(
				err, h.VolumeHealthChecker.CheckVolumeHealth(availableChunk.ID.VolumeID()).State,
			),
			availableChunk.ID,
			availableChunk.Version,
			availableChunk.Size,
		)
	}

	return &AppendChunkOutput{
		Chunk:        newAvailableChunk,
		VolumeHealth: h.VolumeHealthChecker.CheckVolumeHealth(availableChunk.ID.VolumeID()),
	}, nil
}
