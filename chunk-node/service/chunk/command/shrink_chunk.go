package command

import (
	"context"
	"fmt"
	"time"

	adaptervolumetelemetry "github.com/amari/mithril/chunk-node/adapter/volume/telemetry"
	"github.com/amari/mithril/chunk-node/chunkerrors"
	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/port/chunk"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
	"github.com/amari/mithril/chunk-node/service/volume"
	"github.com/amari/mithril/chunk-node/volumeerrors"
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
	Repo                      chunk.ChunkRepository
	VolumeAdmissionController portvolume.VolumeAdmissionController
	VolumeHealthChecker       portvolume.VolumeHealthChecker
	VolumeManager             *volume.VolumeManager
	VolumeTelemetryProvider   portvolume.VolumeTelemetryProvider
	NowFunc                   func() time.Time
}

func NewShrinkChunkHandler(
	repo chunk.ChunkRepository,
	volumeAdmissionController portvolume.VolumeAdmissionController,
	volumeHealthChecker portvolume.VolumeHealthChecker,
	volumeManager *volume.VolumeManager,
	volumeTelemetryProvider portvolume.VolumeTelemetryProvider,
) *ShrinkChunkHandler {
	return &ShrinkChunkHandler{
		Repo:                      repo,
		VolumeAdmissionController: volumeAdmissionController,
		VolumeHealthChecker:       volumeHealthChecker,
		VolumeManager:             volumeManager,
		VolumeTelemetryProvider:   volumeTelemetryProvider,
		NowFunc:                   time.Now,
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
		return nil, chunkerrors.WithChunk(
			volumeerrors.WithState(chunkerrors.ErrVersionMismatch, h.VolumeHealthChecker.CheckVolumeHealth(availableChunk.ID.VolumeID()).State),
			availableChunk.ID,
			availableChunk.Version,
			availableChunk.Size,
		)
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
		zerolog.Ctx(ctx).Error().Err(err).Msg("failed to get volume handle")

		return nil, chunkerrors.WithChunk(
			err,
			availableChunk.ID,
			availableChunk.Version,
			availableChunk.Size,
		)
	}

	if err := vol.Chunks().ShrinkChunkTailSlack(ctx, availableChunk.ID, availableChunk.Size, input.MaxTailSlackSize); err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("failed to shrink chunk tail slack")

		return nil, chunkerrors.WithChunk(
			volumeerrors.WithState(err, h.VolumeHealthChecker.CheckVolumeHealth(availableChunk.ID.VolumeID()).State),
			availableChunk.ID,
			availableChunk.Version,
			availableChunk.Size,
		)
	}

	return &ShrinkChunkOutput{
		Chunk:        availableChunk,
		VolumeHealth: h.VolumeHealthChecker.CheckVolumeHealth(availableChunk.ID.VolumeID()),
	}, nil
}
