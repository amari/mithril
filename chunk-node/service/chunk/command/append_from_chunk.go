package command

import (
	"context"
	"fmt"
	"time"

	adaptervolumetelemetry "github.com/amari/mithril/chunk-node/adapter/volume/telemetry"
	"github.com/amari/mithril/chunk-node/chunkerrors"
	"github.com/amari/mithril/chunk-node/domain"
	portchunk "github.com/amari/mithril/chunk-node/port/chunk"
	portremotechunknode "github.com/amari/mithril/chunk-node/port/remotechunknode"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
	"github.com/amari/mithril/chunk-node/service/volume"
	"github.com/amari/mithril/chunk-node/volumeerrors"
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
	Chunk        *domain.AvailableChunk
	VolumeHealth *domain.VolumeHealth
}

type AppendFromChunkHandler struct {
	Repo                      portchunk.ChunkRepository
	VolumeAdmissionController portvolume.VolumeAdmissionController
	VolumeHealthChecker       portvolume.VolumeHealthChecker
	VolumeManager             *volume.VolumeManager
	VolumeTelemetryProvider   portvolume.VolumeTelemetryProvider
	RemoteChunkClient         portremotechunknode.RemoteChunkClient
	NowFunc                   func() time.Time
}

func NewAppendFromChunkHandler(
	repo portchunk.ChunkRepository,
	volumeAdmissionController portvolume.VolumeAdmissionController,
	volumeHealthChecker portvolume.VolumeHealthChecker,
	volumeManager *volume.VolumeManager,
	volumeTelemetryProvider portvolume.VolumeTelemetryProvider,
	remoteChunkClient portremotechunknode.RemoteChunkClient,
) *AppendFromChunkHandler {
	return &AppendFromChunkHandler{
		Repo:                      repo,
		VolumeAdmissionController: volumeAdmissionController,
		VolumeHealthChecker:       volumeHealthChecker,
		VolumeManager:             volumeManager,
		VolumeTelemetryProvider:   volumeTelemetryProvider,
		RemoteChunkClient:         remoteChunkClient,
		NowFunc:                   time.Now,
	}
}

func (h *AppendFromChunkHandler) HandleAppendFromChunk(ctx context.Context, input *AppendFromChunkInput) (*AppendFromChunkOutput, error) {
	remoteChunkID, err := portchunk.ChunkIDFromBytes(input.RemoteChunkID)
	if err != nil {
		return nil, fmt.Errorf("invalid remote chunk ID: %w", err)
	}

	c, err := h.Repo.GetByWriterKey(ctx, input.WriteKey)
	if err != nil {
		return nil, err
	}

	availableChunk, ok := c.AsAvailable()
	if !ok {
		return nil, fmt.Errorf("%w: chunk with write key %x is not available", chunkerrors.ErrWrongState, input.WriteKey)
	}

	volumeID := availableChunk.ID.VolumeID()

	// Add volume telemetry to context
	ctx = adaptervolumetelemetry.WithVolumeTelemetry(ctx, volumeID, h.VolumeTelemetryProvider)

	// perform admission control check before writing to the volume to avoid writing to a volume that is not healthy enough to accept writes
	if err := h.VolumeAdmissionController.AdmitWrite(volumeID); err != nil {
		return nil, chunkerrors.WithChunk(
			err,
			availableChunk.ID,
			availableChunk.Version,
			availableChunk.Size,
		)
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

	// read the remote chunk data
	remoteChunkReader, err := h.RemoteChunkClient.ReadChunkRange(ctx, remoteChunkID, input.RemoteChunkOffset, input.RemoteChunkLength)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("failed to create remote chunk reader")

		return nil, chunkerrors.WithChunk(
			err,
			availableChunk.ID,
			availableChunk.Version,
			availableChunk.Size,
		)
	}
	defer remoteChunkReader.Close()

	// append the remote chunk data to the volume
	if err := vol.Chunks().AppendChunk(ctx, availableChunk.ID, availableChunk.Size, remoteChunkReader, input.RemoteChunkLength, input.MinTailSlackSize); err != nil {
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

	// update the available chunk metadata with the new size and version
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

		return nil, chunkerrors.WithChunk(
			volumeerrors.WithState(
				err, h.VolumeHealthChecker.CheckVolumeHealth(availableChunk.ID.VolumeID()).State,
			),
			availableChunk.ID,
			availableChunk.Version,
			availableChunk.Size,
		)
	}

	return &AppendFromChunkOutput{
		Chunk:        newAvailableChunk,
		VolumeHealth: h.VolumeHealthChecker.CheckVolumeHealth(availableChunk.ID.VolumeID()),
	}, nil
}
