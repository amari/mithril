package applicationcommands

import (
	"context"
	"time"

	applicationservices "github.com/amari/mithril/mithril-node-go/internal/application/services"
	"github.com/amari/mithril/mithril-node-go/internal/domain"
	"github.com/rs/zerolog"
)

type DeleteChunkCommand struct {
	WriterKey []byte
}

type DeleteChunkCommandResponse struct {
	Chunk        *domain.DeletedChunk
	VolumeStatus domain.VolumeStatus
}

type DeleteChunkCommandHandler interface {
	Handle(ctx context.Context, cmd *DeleteChunkCommand) (*DeleteChunkCommandResponse, error)
}

type deleteChunkCommandHandler struct {
	nowFunc         func() time.Time
	chunkRepository domain.ChunkRepository
	volumeService   applicationservices.VolumeService
}

func NewDeleteChunkCommandHandler(
	nowFunc func() time.Time,
	chunkRepository domain.ChunkRepository,
	volumeService applicationservices.VolumeService,
) DeleteChunkCommandHandler {
	return &deleteChunkCommandHandler{
		nowFunc:         nowFunc,
		chunkRepository: chunkRepository,
		volumeService:   volumeService,
	}
}

func (h *deleteChunkCommandHandler) Handle(ctx context.Context, cmd *DeleteChunkCommand) (*DeleteChunkCommandResponse, error) {
	chunk, err := h.chunkRepository.GetWithWriterKey(ctx, cmd.WriterKey)
	if err != nil {
		return nil, err
	}

	readyChunk, ok := chunk.(*domain.ReadyChunk)
	if !ok {
		return nil, domain.ErrChunkWrongState
	}

	volume, err := h.volumeService.GetVolume(readyChunk.ID().VolumeID())
	if err != nil {
		return nil, WithChunk(err, readyChunk)
	}

	logger := zerolog.Ctx(ctx)
	if logger != nil {
		newLogger := logger.With().Fields(volume.GetStructuredLoggingFieldsProvider().Get()).Logger()
		ctx = newLogger.WithContext(ctx)
	}

	if err := volume.GetChunkStorage().Delete(ctx, readyChunk.ID()); err != nil {
		return nil, WithChunkAndVolumeStatus(err, readyChunk, volume.GetStatusProvider().Get())
	}

	newDeletedChunk := domain.NewDeletedChunk(readyChunk.ID(), readyChunk.WriterKey(), readyChunk.CreatedAt(), h.nowFunc())

	if err := h.chunkRepository.Upsert(ctx, newDeletedChunk); err != nil {
		return nil, WithChunkAndVolumeStatus(err, readyChunk, volume.GetStatusProvider().Get())
	}

	return &DeleteChunkCommandResponse{
		Chunk:        newDeletedChunk,
		VolumeStatus: volume.GetStatusProvider().Get(),
	}, nil
}
