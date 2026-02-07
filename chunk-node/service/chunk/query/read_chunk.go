package query

import (
	"context"

	"github.com/amari/mithril/chunk-node/domain"
	chunkstoreerrors "github.com/amari/mithril/chunk-node/errors"
	"github.com/amari/mithril/chunk-node/port"
	"github.com/amari/mithril/chunk-node/port/chunk"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
	"github.com/amari/mithril/chunk-node/service/volume"
	"github.com/rs/zerolog"
)

type ReadChunkInput struct {
	ChunkID   []byte
	WriterKey []byte
}

type ReadChunkOutput struct {
	Chunk        *domain.AvailableChunk
	VolumeHealth *domain.VolumeHealth

	Handle                port.Chunk
	CheckVolumeHealthFunc func() (*domain.VolumeHealth, error)
}

type ReadChunkHandler struct {
	Repo                chunk.ChunkRepository
	VolumeManager       *volume.VolumeManager
	VolumeHealthChecker portvolume.VolumeHealthChecker
}

func NewReadChunkHandler(
	repo chunk.ChunkRepository,
	volumeManager *volume.VolumeManager,
	volumeHealthChecker portvolume.VolumeHealthChecker,
) *ReadChunkHandler {
	return &ReadChunkHandler{
		Repo:                repo,
		VolumeManager:       volumeManager,
		VolumeHealthChecker: volumeHealthChecker,
	}
}

func (h *ReadChunkHandler) HandleReadChunk(ctx context.Context, input *ReadChunkInput) (*ReadChunkOutput, error) {
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
		return nil, chunkstoreerrors.ErrChunkNotFound
	}

	availableChunk, ok := c.AsAvailable()
	if !ok {
		return nil, chunkstoreerrors.ErrChunkWrongState
	}

	volumeID := availableChunk.ChunkID().VolumeID()

	vol, err := h.VolumeManager.GetVolumeByID(volumeID)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("failed to get volume handle")

		return nil, err
	}

	// check the volume health before returning the chunk handle
	// to avoid reading from a failed volume
	volumeHealth, err := h.VolumeHealthChecker.CheckVolumeHealth(availableChunk.ChunkID().VolumeID())
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("failed to check volume health")

		return nil, err
	}
	if volumeHealth.State == domain.VolumeStateFailed {
		zerolog.Ctx(ctx).Error().Msg("volume is in failed state")

		return nil, chunkstoreerrors.ErrVolumeFailed
	}

	handle, err := vol.Chunks().OpenChunk(ctx, availableChunk.ID)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("failed to open chunk from volume")

		return nil, err
	}

	return &ReadChunkOutput{
		Chunk:        availableChunk,
		VolumeHealth: volumeHealth,
		Handle:       handle,
		CheckVolumeHealthFunc: func() (*domain.VolumeHealth, error) {
			return h.VolumeHealthChecker.CheckVolumeHealth(volumeID)
		},
	}, nil
}
