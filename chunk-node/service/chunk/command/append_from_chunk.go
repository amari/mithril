package command

import (
	"context"
	"fmt"
	"time"

	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/errors"
	"github.com/amari/mithril/chunk-node/port/chunk"
	portremotechunknode "github.com/amari/mithril/chunk-node/port/remotechunknode"
	"github.com/amari/mithril/chunk-node/service/volume"
	"github.com/rs/zerolog"
)

type AppendFromChunkInput struct {
	WriteKey         []byte
	ExpectedVersion  uint64
	MinTailSlackSize int64

	RemoteChunkID     []byte
	RemoteChunkOffset int64
	RemoteChunkLength int64
}

type AppendFromChunkOutput struct {
	Chunk *domain.AvailableChunk
}

type AppendFromChunkHandler struct {
	Repo              chunk.ChunkRepository
	VolumeManager     *volume.VolumeManager
	RemoteChunkClient portremotechunknode.RemoteChunkClient
	NowFunc           func() time.Time
}

func NewAppendFromChunkHandler(
	repo chunk.ChunkRepository,
	volumeManager *volume.VolumeManager,
	remoteChunkClient portremotechunknode.RemoteChunkClient,
) *AppendFromChunkHandler {
	return &AppendFromChunkHandler{
		Repo:              repo,
		VolumeManager:     volumeManager,
		RemoteChunkClient: remoteChunkClient,
		NowFunc:           time.Now,
	}
}

func (h *AppendFromChunkHandler) HandleAppendFromChunk(ctx context.Context, input *AppendFromChunkInput) (*AppendFromChunkOutput, error) {
	remoteChunkID, err := chunk.ChunkIDFromBytes(input.RemoteChunkID)
	if err != nil {
		return nil, fmt.Errorf("invalid remote chunk ID: %w", err)
	}

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

	remoteChunkReader, err := h.RemoteChunkClient.ReadChunkRange(ctx, remoteChunkID, input.RemoteChunkOffset, input.RemoteChunkLength)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("failed to create remote chunk reader")

		return nil, &errors.ChunkError{
			Cause: err,
			Chunk: &errors.Chunk{
				ID:      availableChunk.ID[:],
				Version: availableChunk.Version,
				Size:    availableChunk.Size,
			},
		}
	}
	defer remoteChunkReader.Close()

	if err := vol.Chunks().AppendChunk(ctx, availableChunk.ID, availableChunk.Size, remoteChunkReader, input.RemoteChunkLength, input.MinTailSlackSize); err != nil {
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
		Size:      availableChunk.Size + input.RemoteChunkLength,
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

	return &AppendFromChunkOutput{
		Chunk: newAvailableChunk,
	}, nil
}
