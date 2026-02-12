package query

import (
	"context"

	adaptervolumetelemetry "github.com/amari/mithril/chunk-node/adapter/volume/telemetry"
	"github.com/amari/mithril/chunk-node/chunkerrors"
	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/port"
	"github.com/amari/mithril/chunk-node/port/chunk"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
	"github.com/amari/mithril/chunk-node/service/admission"
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
	CheckVolumeHealthFunc func() *domain.VolumeHealth
}

type ReadChunkHandler struct {
	Repo                    chunk.ChunkRepository
	VolumeManager           *volume.VolumeManager
	VolumeHealthChecker     portvolume.VolumeHealthChecker
	VolumeTelemetryProvider portvolume.VolumeTelemetryProvider
}

func NewReadChunkHandler(
	repo chunk.ChunkRepository,
	volumeManager *volume.VolumeManager,
	volumeHealthChecker portvolume.VolumeHealthChecker,
	volumeTelemetryProvider portvolume.VolumeTelemetryProvider,
) *ReadChunkHandler {
	return &ReadChunkHandler{
		Repo:                    repo,
		VolumeManager:           volumeManager,
		VolumeHealthChecker:     volumeHealthChecker,
		VolumeTelemetryProvider: volumeTelemetryProvider,
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
		return nil, chunkerrors.ErrNotFound
	}

	availableChunk, ok := c.AsAvailable()
	if !ok {
		return nil, chunkerrors.ErrWrongState
	}

	volumeID := availableChunk.ChunkID().VolumeID()

	// Add volume telemetry to context
	ctx = adaptervolumetelemetry.WithVolumeTelemetry(ctx, volumeID, h.VolumeTelemetryProvider)

	vol, err := h.VolumeManager.GetVolumeByID(volumeID)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("failed to get volume handle")

		return nil, err
	}

	/// check the volume health
	volumeHealth := h.VolumeHealthChecker.CheckVolumeHealth(availableChunk.ID.VolumeID())

	// perform admission control check before reading from the volume to avoid reading from a volume that is not healthy enough to accept reads
	if err := admission.AdmitReadWithVolumeHealth(ctx, volumeHealth); err != nil {
		return nil, chunkerrors.WithChunk(
			err,
			availableChunk.ID,
			availableChunk.Version,
			availableChunk.Size,
		)
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
		CheckVolumeHealthFunc: func() *domain.VolumeHealth {
			return h.VolumeHealthChecker.CheckVolumeHealth(volumeID)
		},
	}, nil
}
