package applicationcommands

import (
	"context"
	"time"

	applicationservices "github.com/amari/mithril/mithril-node-go/internal/application/services"
	"github.com/amari/mithril/mithril-node-go/internal/domain"
	"github.com/rs/zerolog"
)

type ShrinkChunkToFitCommand struct {
	WriterKey          []byte
	ExpectedVersion    uint64
	MaxTailSlackLength int64
}

type ShrinkChunkToFitCommandResponse struct {
	Chunk        *domain.ReadyChunk
	VolumeStatus domain.VolumeStatus
}

type ShrinkChunkToFitCommandHandler interface {
	Handle(ctx context.Context, cmd *ShrinkChunkToFitCommand) (*ShrinkChunkToFitCommandResponse, error)
}

type shrinkChunkToFitCommandHandler struct {
	nowFunc         func() time.Time
	chunkRepository domain.ChunkRepository
	volumeService   applicationservices.VolumeService
}

func NewShrinkChunkToFitCommandHandler(
	nowFunc func() time.Time,
	chunkRepository domain.ChunkRepository,
	volumeService applicationservices.VolumeService,
) ShrinkChunkToFitCommandHandler {
	return &shrinkChunkToFitCommandHandler{
		nowFunc:         nowFunc,
		chunkRepository: chunkRepository,
		volumeService:   volumeService,
	}
}

func (h *shrinkChunkToFitCommandHandler) Handle(ctx context.Context, cmd *ShrinkChunkToFitCommand) (*ShrinkChunkToFitCommandResponse, error) {
	chunk, err := h.chunkRepository.GetWithWriterKey(ctx, cmd.WriterKey)
	if err != nil {
		return nil, err
	}

	readyChunk, ok := chunk.(*domain.ReadyChunk)
	if !ok {
		return nil, domain.ErrChunkWrongState
	}

	version, ok := readyChunk.Version()
	if !ok || version != cmd.ExpectedVersion {
		return nil, WithChunk(domain.ErrChunkVersionMismatch, readyChunk)
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

	knownSize, _ := readyChunk.Size()
	if err := volume.GetChunkStorage().ShrinkToFit(ctx, readyChunk.ID(), knownSize, domain.ShrinkChunkToFitOptions{
		MaxTailSlackLength: cmd.MaxTailSlackLength,
	}); err != nil {
		return nil, WithChunkAndVolumeStatus(err, readyChunk, volume.GetStatusProvider().Get())
	}

	return &ShrinkChunkToFitCommandResponse{
		Chunk:        readyChunk,
		VolumeStatus: volume.GetStatusProvider().Get(),
	}, nil
}
