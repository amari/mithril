package query

import (
	"context"

	adaptervolumetelemetry "github.com/amari/mithril/chunk-node/adapter/volume/telemetry"
	"github.com/amari/mithril/chunk-node/chunkerrors"
	"github.com/amari/mithril/chunk-node/domain"
	portchunk "github.com/amari/mithril/chunk-node/port/chunk"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
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
	Repo                      portchunk.ChunkRepository
	VolumeAdmissionController portvolume.VolumeAdmissionController
	VolumeHealthChecker       portvolume.VolumeHealthChecker
	VolumeTelemetryProvider   portvolume.VolumeTelemetryProvider
}

func NewStatChunkHandler(
	repo portchunk.ChunkRepository,
	volumeAdmissionController portvolume.VolumeAdmissionController,
	volumeHealthChecker portvolume.VolumeHealthChecker,
	volumeTelemetryProvider portvolume.VolumeTelemetryProvider,
) *StatChunkHandler {
	return &StatChunkHandler{
		Repo:                      repo,
		VolumeAdmissionController: volumeAdmissionController,
		VolumeHealthChecker:       volumeHealthChecker,
		VolumeTelemetryProvider:   volumeTelemetryProvider,
	}
}

func (h *StatChunkHandler) HandleStatChunk(ctx context.Context, input *StatChunkInput) (*StatChunkOutput, error) {
	var (
		c   domain.Chunk
		err error
	)

	if len(input.ChunkID) > 0 {
		id, err := portchunk.ChunkIDFromBytes(input.ChunkID)
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

	volumeID := availableChunk.ID.VolumeID()

	// Add volume telemetry to context
	ctx = adaptervolumetelemetry.WithVolumeTelemetry(ctx, volumeID, h.VolumeTelemetryProvider)

	// perform admission control check before reading from the volume to avoid reading from a volume that is not healthy enough to accept reads
	if err := h.VolumeAdmissionController.AdmitStat(volumeID); err != nil {
		return nil, chunkerrors.WithChunk(
			err,
			availableChunk.ID,
			availableChunk.Version,
			availableChunk.Size,
		)
	}

	return &StatChunkOutput{
		Chunk:        availableChunk,
		VolumeHealth: h.VolumeHealthChecker.CheckVolumeHealth(availableChunk.ChunkID().VolumeID()),
	}, nil
}
