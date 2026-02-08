package command

import (
	"context"
	"errors"
	"time"

	"github.com/amari/mithril/chunk-node/chunkerrors"
	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/port/chunk"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
	"github.com/amari/mithril/chunk-node/service/admission"
	"github.com/amari/mithril/chunk-node/service/volume"
)

type DeleteChunkInput struct {
	WriteKey []byte
}

type DeleteChunkOutput struct {
	Chunk        *domain.DeletedChunk
	VolumeHealth *domain.VolumeHealth
}

type DeleteChunkHandler struct {
	Repo                chunk.ChunkRepository
	VolumeHealthChecker portvolume.VolumeHealthChecker
	VolumeManager       *volume.VolumeManager
	NowFunc             func() time.Time
}

func NewDeleteChunkHandler(
	repo chunk.ChunkRepository,
	volumeHealthChecker portvolume.VolumeHealthChecker,
	volumeManager *volume.VolumeManager,
) *DeleteChunkHandler {
	return &DeleteChunkHandler{
		Repo:                repo,
		VolumeHealthChecker: volumeHealthChecker,
		VolumeManager:       volumeManager,
		NowFunc:             time.Now,
	}
}

func (h *DeleteChunkHandler) HandleDeleteChunk(ctx context.Context, input *DeleteChunkInput) (*DeleteChunkOutput, error) {
	c, err := h.Repo.GetByWriterKey(ctx, input.WriteKey)
	if err != nil {
		return nil, err
	}

	availableChunk, ok := c.AsAvailable()
	if !ok {
		return nil, chunkerrors.ErrWrongState
	}

	vol, err := h.VolumeManager.GetVolumeByID(availableChunk.ID.VolumeID())
	if err != nil {
		return nil, err
	}

	// check the volume health
	volumeHealth := h.VolumeHealthChecker.CheckVolumeHealth(c.ChunkID().VolumeID())

	// perform admission control check before writing to the volume to avoid writing to a volume that is not healthy enough to accept writes
	if err := admission.AdmitWriteWithVolumeHealth(ctx, volumeHealth); err != nil {
		return nil, chunkerrors.WithChunk(
			err,
			c.ChunkID(),
			c.ChunkVersion(),
			c.ChunkSize(),
		)
	}

	if err := vol.Chunks().DeleteChunk(ctx, availableChunk.ID); err != nil {
		if !errors.Is(err, chunkerrors.ErrNotFound) {
			return nil, err
		}
	}

	deletedChunk := &domain.DeletedChunk{
		ID:        availableChunk.ID,
		WriterKey: availableChunk.WriterKey,
		DeletedAt: h.NowFunc(),
	}

	if err := h.Repo.Store(ctx, deletedChunk); err != nil {
		if !errors.Is(err, chunkerrors.ErrNotFound) {
			return nil, err
		}
	}

	return &DeleteChunkOutput{
		Chunk: deletedChunk,
	}, nil
}
