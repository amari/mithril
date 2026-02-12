package command

import (
	"context"
	"errors"
	"time"

	adaptervolumetelemetry "github.com/amari/mithril/chunk-node/adapter/volume/telemetry"
	"github.com/amari/mithril/chunk-node/chunkerrors"
	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/port/chunk"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
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
	Repo                      chunk.ChunkRepository
	VolumeAdmissionController portvolume.VolumeAdmissionController
	VolumeHealthChecker       portvolume.VolumeHealthChecker
	VolumeManager             *volume.VolumeManager
	VolumeTelemetryProvider   portvolume.VolumeTelemetryProvider
	NowFunc                   func() time.Time
}

func NewDeleteChunkHandler(
	repo chunk.ChunkRepository,
	volumeAdmissionController portvolume.VolumeAdmissionController,
	volumeHealthChecker portvolume.VolumeHealthChecker,
	volumeManager *volume.VolumeManager,
	volumeTelemetryProvider portvolume.VolumeTelemetryProvider,
) *DeleteChunkHandler {
	return &DeleteChunkHandler{
		Repo:                      repo,
		VolumeAdmissionController: volumeAdmissionController,
		VolumeHealthChecker:       volumeHealthChecker,
		VolumeManager:             volumeManager,
		VolumeTelemetryProvider:   volumeTelemetryProvider,
		NowFunc:                   time.Now,
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

	// Add volume telemetry to context
	ctx = adaptervolumetelemetry.WithVolumeTelemetry(ctx, availableChunk.ID.VolumeID(), h.VolumeTelemetryProvider)

	// perform admission control check before writing to the volume to avoid writing to a volume that is not healthy enough to accept writes
	if err := h.VolumeAdmissionController.AdmitWrite(availableChunk.ID.VolumeID()); err != nil {
		return nil, chunkerrors.WithChunk(
			err,
			availableChunk.ID,
			availableChunk.Version,
			availableChunk.Size,
		)
	}

	vol, err := h.VolumeManager.GetVolumeByID(availableChunk.ID.VolumeID())
	if err != nil {
		return nil, err
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
		Chunk:        deletedChunk,
		VolumeHealth: h.VolumeHealthChecker.CheckVolumeHealth(availableChunk.ID.VolumeID()),
	}, nil
}
