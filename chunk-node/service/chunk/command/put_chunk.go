package command

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/amari/mithril/chunk-node/adapter/volume/picker"
	adaptervolumetelemetry "github.com/amari/mithril/chunk-node/adapter/volume/telemetry"
	"github.com/amari/mithril/chunk-node/chunkerrors"
	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/port"
	portchunk "github.com/amari/mithril/chunk-node/port/chunk"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
	"github.com/amari/mithril/chunk-node/service/volume"
)

type PutChunkInput struct {
	WriteKey         []byte
	MinTailSlackSize int64
	Body             io.Reader
	BodySize         int64
}

type PutChunkOutput struct {
	Chunk        *domain.AvailableChunk
	VolumeHealth *domain.VolumeHealth
}

type PutChunkHandler struct {
	Repo                      portchunk.ChunkRepository
	IDGen                     port.ChunkIDGenerator
	VolumeAdmissionController portvolume.VolumeAdmissionController
	VolumeManager             *volume.VolumeManager
	VolumePicker              portvolume.VolumePicker
	NowFunc                   func() time.Time
	NodeIdentityRepository    port.NodeIdentityRepository
	VolumeHealthChecker       portvolume.VolumeHealthChecker
	VolumeIDToStatsIndex      portvolume.VolumeIDToStatsIndex
	VolumeTelemetryProvider   portvolume.VolumeTelemetryProvider
}

func NewPutChunkHandler(
	repo portchunk.ChunkRepository,
	idGen port.ChunkIDGenerator,
	volumeAdmissionController portvolume.VolumeAdmissionController,
	volumeManager *volume.VolumeManager,
	volumePicker portvolume.VolumePicker,
	nodeIdentityRepository port.NodeIdentityRepository,
	volumeHealthChecker portvolume.VolumeHealthChecker,
	volumeIDToStatsIndex portvolume.VolumeIDToStatsIndex,
	volumeTelemetryProvider portvolume.VolumeTelemetryProvider,
) *PutChunkHandler {
	return &PutChunkHandler{
		Repo:                      repo,
		IDGen:                     idGen,
		VolumeAdmissionController: volumeAdmissionController,
		VolumeManager:             volumeManager,
		VolumePicker:              volumePicker,
		NowFunc:                   time.Now,
		NodeIdentityRepository:    nodeIdentityRepository,
		VolumeHealthChecker:       volumeHealthChecker,
		VolumeIDToStatsIndex:      volumeIDToStatsIndex,
		VolumeTelemetryProvider:   volumeTelemetryProvider,
	}
}

func (h *PutChunkHandler) HandlePutChunk(ctx context.Context, input *PutChunkInput) (*PutChunkOutput, error) {
	chunk, err := h.Repo.GetByWriterKey(ctx, input.WriteKey)
	if err != nil && !errors.Is(err, chunkerrors.ErrNotFound) {
		return nil, err
	}

	// Idempotency: write key already exists
	if chunk != nil {
		switch c := chunk.(type) {
		case *domain.AvailableChunk:
			return h.handleExistingAvailable(c)

		case *domain.TempChunk:
			return h.handleExistingTemp(ctx, c, input)

		default:
			// Unknown state, treat as not found
		}
	}

	// Fresh create
	return h.createFreshChunk(ctx, input)
}

func (h *PutChunkHandler) handleExistingAvailable(c *domain.AvailableChunk) (*PutChunkOutput, error) {
	return &PutChunkOutput{Chunk: c, VolumeHealth: h.VolumeHealthChecker.CheckVolumeHealth(c.ID.VolumeID())}, nil
}

func (h *PutChunkHandler) handleExistingTemp(
	ctx context.Context,
	c *domain.TempChunk,
	input *PutChunkInput,
) (*PutChunkOutput, error) {

	// Add volume telemetry to context
	ctx = adaptervolumetelemetry.WithVolumeTelemetry(ctx, c.ID.VolumeID(), h.VolumeTelemetryProvider)

	// perform admission control check before writing to the volume to avoid writing to a volume that is not healthy enough to accept writes
	if err := h.VolumeAdmissionController.AdmitWrite(c.ID.VolumeID()); err != nil {
		return nil, chunkerrors.WithChunk(
			err,
			c.ChunkID(),
			c.ChunkVersion(),
			c.ChunkSize(),
		)
	}

	volHandle, err := h.VolumeManager.GetVolumeByID(c.ID.VolumeID())
	if err != nil {
		return nil, err
	}

	exists, _ := volHandle.Chunks().ChunkExists(ctx, c.ID)
	if exists {
		available := &domain.AvailableChunk{
			ID:        c.ID,
			WriterKey: c.WriterKey,
			Size:      0,
			Version:   1,
			CreatedAt: c.CreatedAt,
			UpdatedAt: h.NowFunc(),
		}

		if err := h.Repo.Store(ctx, available); err != nil {
			return nil, fmt.Errorf("failed to upsert available chunk: %w", err)
		}

		return &PutChunkOutput{Chunk: available, VolumeHealth: h.VolumeHealthChecker.CheckVolumeHealth(available.ID.VolumeID())}, nil
	}

	// Recreate temp metadata
	temp := &domain.TempChunk{
		ID:        c.ID,
		WriterKey: c.WriterKey,
		CreatedAt: c.CreatedAt,
		ExpiresAt: h.NowFunc().Add(168 * time.Hour),
	}

	if err := h.Repo.Store(ctx, temp); err != nil {
		return nil, fmt.Errorf("failed to insert temp chunk: %w", err)
	}

	// Create physical chunk
	if err := volHandle.Chunks().PutChunk(ctx, temp.ID, input.Body, input.BodySize, input.MinTailSlackSize); err != nil {
		return nil, fmt.Errorf("failed to create chunk in volume: %w", err)
	}

	available := &domain.AvailableChunk{
		ID:        c.ID,
		WriterKey: c.WriterKey,
		Size:      0,
		Version:   1,
		CreatedAt: c.CreatedAt,
		UpdatedAt: h.NowFunc(),
	}

	if err := h.Repo.Store(ctx, available); err != nil {
		return nil, fmt.Errorf("failed to upsert available chunk: %w", err)
	}

	return &PutChunkOutput{Chunk: available, VolumeHealth: h.VolumeHealthChecker.CheckVolumeHealth(available.ID.VolumeID())}, nil
}

func (h *PutChunkHandler) createFreshChunk(
	ctx context.Context,
	input *PutChunkInput,
) (*PutChunkOutput, error) {
	nodeIdentity, err := h.NodeIdentityRepository.LoadNodeIdentity(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get node identity: %w", err)
	}

	vol, err := h.VolumePicker.PickVolumeID(portvolume.PickVolumeIDOptions{
		Filter: picker.ChainPickFilter(
			&picker.HealthPickFilter{
				HealthChecker: h.VolumeHealthChecker,
			},
			&picker.AvailableSpacePickFilter{
				MinFreeSpaceBytes:    max(0, input.MinTailSlackSize),
				VolumeIDToStatsIndex: h.VolumeIDToStatsIndex,
			},
		),
	})
	if err != nil {
		return nil, err
	}

	// Add volume telemetry to context
	ctx = adaptervolumetelemetry.WithVolumeTelemetry(ctx, vol, h.VolumeTelemetryProvider)

	// perform admission control check before writing to the volume to avoid writing to a volume that is not healthy enough to accept writes
	if err := h.VolumeAdmissionController.AdmitWrite(vol); err != nil {
		return nil, err
	}

	volHandle, err := h.VolumeManager.GetVolumeByID(vol)
	if err != nil {
		return nil, err
	}

	chunkID, err := h.IDGen.NextID(nodeIdentity.NodeID, vol)
	if err != nil {
		return nil, err
	}

	now := h.NowFunc()

	temp := &domain.TempChunk{
		ID:        chunkID,
		WriterKey: input.WriteKey,
		CreatedAt: now,
		ExpiresAt: now.Add(168 * time.Hour),
	}

	if err := h.Repo.Store(ctx, temp); err != nil {
		return nil, fmt.Errorf("failed to insert temp chunk: %w", err)
	}

	if err := volHandle.Chunks().PutChunk(ctx, temp.ID, input.Body, input.BodySize, input.MinTailSlackSize); err != nil {
		return nil, fmt.Errorf("failed to create chunk in volume: %w", err)
	}

	available := &domain.AvailableChunk{
		ID:        temp.ID,
		WriterKey: temp.WriterKey,
		Size:      0,
		Version:   1,
		CreatedAt: temp.CreatedAt,
		UpdatedAt: h.NowFunc(),
	}

	if err := h.Repo.Store(ctx, available); err != nil {
		return nil, fmt.Errorf("failed to upsert available chunk: %w", err)
	}

	return &PutChunkOutput{Chunk: available, VolumeHealth: h.VolumeHealthChecker.CheckVolumeHealth(available.ID.VolumeID())}, nil
}
