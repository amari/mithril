package applicationcommands

import (
	"context"
	"errors"
	"io"
	"time"

	applicationservices "github.com/amari/mithril/mithril-node-go/internal/application/services"
	"github.com/amari/mithril/mithril-node-go/internal/domain"
	"github.com/rs/zerolog"
)

type PutChunkCommand struct {
	WriterKey          []byte
	MinTailSlackLength int64

	Body io.Reader
	N    int64
}

type PutChunkCommandResponse struct {
	Chunk        *domain.ReadyChunk
	VolumeStatus domain.VolumeStatus
}

type PutChunkCommandHandler interface {
	Handle(ctx context.Context, cmd *PutChunkCommand) (*PutChunkCommandResponse, error)
}

type putChunkCommandHandler struct {
	nowFunc          func() time.Time
	chunkIDGenerator domain.ChunkIDGenerator
	chunkRepository  domain.ChunkRepository
	volumeChooser    domain.VolumeChooser
	volumeService    applicationservices.VolumeService
}

func NewPutChunkCommandHandler(
	nowFunc func() time.Time,
	chunkIDGenerator domain.ChunkIDGenerator,
	chunkRepository domain.ChunkRepository,
	volumeChooser domain.VolumeChooser,
	volumeService applicationservices.VolumeService,
) PutChunkCommandHandler {
	return &putChunkCommandHandler{
		nowFunc:          nowFunc,
		chunkIDGenerator: chunkIDGenerator,
		chunkRepository:  chunkRepository,
		volumeChooser:    volumeChooser,
		volumeService:    volumeService,
	}
}

func (h *putChunkCommandHandler) Handle(ctx context.Context, cmd *PutChunkCommand) (*PutChunkCommandResponse, error) {
	chunk, err := h.chunkRepository.GetWithWriterKey(ctx, cmd.WriterKey)
	if err != nil {
		if errors.Is(err, domain.ErrChunkNotFound) {
			return h.handleNewChunk(ctx, cmd)
		}

		return nil, err
	}

	volume, err := h.volumeService.GetVolume(chunk.ID().VolumeID())
	if err != nil {
		return nil, err
	}

	switch c := chunk.(type) {
	case *domain.PendingChunk:
		return h.handlePendingChunk(ctx, cmd, c, volume)
	case *domain.ReadyChunk:
		return h.handleReadyChunk(c, volume)
	case *domain.DeletedChunk:
		// Allow re-using the writer key
		return h.handleNewChunk(ctx, cmd)
	default:
		// Unexpected chunk type
		return h.handleNewChunk(ctx, cmd)
	}
}

func (h *putChunkCommandHandler) handleNewChunk(ctx context.Context, cmd *PutChunkCommand) (*PutChunkCommandResponse, error) {
	volumeID, err := h.volumeChooser.Choose(domain.ChooseVolumeOptions{})
	if err != nil {
		return nil, err
	}

	volume, err := h.volumeService.GetVolume(volumeID)
	if err != nil {
		return nil, err
	}

	logger := zerolog.Ctx(ctx)
	if logger != nil {
		newLogger := logger.With().Fields(volume.GetStructuredLoggingFieldsProvider().Get()).Logger()
		ctx = newLogger.WithContext(ctx)
	}

	// Create the chunk metadata
	newPendingChunkID, err := h.chunkIDGenerator.Generate(volumeID)
	if err != nil {
		return nil, WithVolumeStatus(err, volume.GetStatusProvider().Get())
	}

	now := h.nowFunc()

	newPendingChunk := domain.NewPendingChunk(newPendingChunkID, cmd.WriterKey, now, now.Add(168*time.Hour))
	if err := h.chunkRepository.Upsert(ctx, newPendingChunk); err != nil {
		return nil, WithVolumeStatus(err, volume.GetStatusProvider().Get())
	}

	// Create the chunk in storage
	if err := volume.GetChunkStorage().Put(ctx, newPendingChunk.ID(), cmd.Body, cmd.N, domain.PutChunkOptions{
		MinTailSlackLength: cmd.MinTailSlackLength,
	}); err != nil {
		return nil, WithVolumeStatus(err, volume.GetStatusProvider().Get())
	}

	// Update metadata
	newReadyChunk := domain.NewReadyChunk(newPendingChunk.ID(), newPendingChunk.WriterKey(), newPendingChunk.CreatedAt(), h.nowFunc(), cmd.N, 1)
	if err := h.chunkRepository.Upsert(ctx, newReadyChunk); err != nil {
		return nil, WithVolumeStatus(err, volume.GetStatusProvider().Get())
	}

	return h.handleReadyChunk(newReadyChunk, volume)
}

func (h *putChunkCommandHandler) handlePendingChunk(ctx context.Context, cmd *PutChunkCommand, pendingChunk *domain.PendingChunk, volume domain.VolumeHandle) (*PutChunkCommandResponse, error) {
	logger := zerolog.Ctx(ctx)
	if logger != nil {
		newLogger := logger.With().Fields(volume.GetStructuredLoggingFieldsProvider().Get()).Logger()
		ctx = newLogger.WithContext(ctx)
	}

	// does the chunk actually exist in storage?
	exists, err := volume.GetChunkStorage().Exists(ctx, pendingChunk.ID())
	if err != nil {
		return nil, WithVolumeStatus(err, volume.GetStatusProvider().Get())
	}
	if exists {
		newReadyChunk := domain.NewReadyChunk(pendingChunk.ID(), pendingChunk.WriterKey(), pendingChunk.CreatedAt(), h.nowFunc(), cmd.N, 1)

		if err := h.chunkRepository.Upsert(ctx, newReadyChunk); err != nil {
			return nil, WithVolumeStatus(err, volume.GetStatusProvider().Get())
		}

		return h.handleReadyChunk(newReadyChunk, volume)
	}

	// Reset the expiration
	newPendingChunk := domain.NewPendingChunk(pendingChunk.ID(), pendingChunk.WriterKey(), pendingChunk.CreatedAt(), h.nowFunc().Add(168*time.Hour))
	if err := h.chunkRepository.Upsert(ctx, newPendingChunk); err != nil {
		return nil, WithVolumeStatus(err, volume.GetStatusProvider().Get())
	}

	// Create the chunk in storage
	if err := volume.GetChunkStorage().Put(ctx, newPendingChunk.ID(), cmd.Body, cmd.N, domain.PutChunkOptions{
		MinTailSlackLength: cmd.MinTailSlackLength,
	}); err != nil {
		return nil, WithVolumeStatus(err, volume.GetStatusProvider().Get())
	}

	// Update metadata
	newReadyChunk := domain.NewReadyChunk(newPendingChunk.ID(), newPendingChunk.WriterKey(), newPendingChunk.CreatedAt(), h.nowFunc(), cmd.N, 1)
	if err := h.chunkRepository.Upsert(ctx, newReadyChunk); err != nil {
		return nil, WithVolumeStatus(err, volume.GetStatusProvider().Get())
	}

	return h.handleReadyChunk(newReadyChunk, volume)
}

func (h *putChunkCommandHandler) handleReadyChunk(readyChunk *domain.ReadyChunk, volume domain.VolumeHandle) (*PutChunkCommandResponse, error) {
	return &PutChunkCommandResponse{
		Chunk:        readyChunk,
		VolumeStatus: volume.GetStatusProvider().Get(),
	}, nil
}
