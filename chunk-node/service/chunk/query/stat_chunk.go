package query

import (
	"context"

	adaptervolumetelemetry "github.com/amari/mithril/chunk-node/adapter/volume/telemetry"
	"github.com/amari/mithril/chunk-node/chunkerrors"
	"github.com/amari/mithril/chunk-node/domain"
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
}

type StatChunkHandler struct {
	Repo                    chunk.ChunkRepository
	VolumeHealthChecker     volume.VolumeHealthChecker
	VolumeTelemetryProvider volume.VolumeTelemetryProvider
}

func NewStatChunkHandler(
	repo chunk.ChunkRepository,
	volumeHealthChecker volume.VolumeHealthChecker,
	volumeTelemetryProvider volume.VolumeTelemetryProvider,
) *StatChunkHandler {
	return &StatChunkHandler{
		Repo:                    repo,
		VolumeHealthChecker:     volumeHealthChecker,
		VolumeTelemetryProvider: volumeTelemetryProvider,
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
		return nil, chunkerrors.ErrWriterKeyMismatch
	}

	availableChunk, ok := c.AsAvailable()
	if !ok {
		return nil, chunkerrors.ErrWrongState
	}

	// Add volume telemetry to context
	ctx = adaptervolumetelemetry.WithVolumeTelemetry(ctx, availableChunk.ID.VolumeID(), h.VolumeTelemetryProvider)

	volumeHealth := h.VolumeHealthChecker.CheckVolumeHealth(availableChunk.ChunkID().VolumeID())

	return &StatChunkOutput{
		Chunk:        availableChunk,
		VolumeHealth: volumeHealth,
	}, nil
}
