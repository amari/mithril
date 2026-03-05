package applicationcommands

import (
	"context"
	"io"
	"time"

	applicationservices "github.com/amari/mithril/mithril-node-go/internal/application/services"
	"github.com/amari/mithril/mithril-node-go/internal/domain"
	"github.com/rs/zerolog"
)

type AppendChunkCommand struct {
	WriterKey          []byte
	ExpectedVersion    uint64
	MinTailSlackLength int64

	Body io.Reader
	N    int64
}

type AppendChunkCommandResponse struct {
	Chunk        *domain.ReadyChunk
	VolumeStatus domain.VolumeStatus
}

type AppendChunkCommandHandler interface {
	Handle(ctx context.Context, cmd *AppendChunkCommand) (*AppendChunkCommandResponse, error)
}

type appendChunkCommandHandler struct {
	nowFunc         func() time.Time
	chunkRepository domain.ChunkRepository
	volumeService   applicationservices.VolumeService
}

func NewAppendChunkCommandHandler(
	nowFunc func() time.Time,
	chunkRepository domain.ChunkRepository,
	volumeService applicationservices.VolumeService,
) AppendChunkCommandHandler {
	return &appendChunkCommandHandler{
		nowFunc:         nowFunc,
		chunkRepository: chunkRepository,
		volumeService:   volumeService,
	}
}

func (h *appendChunkCommandHandler) Handle(ctx context.Context, cmd *AppendChunkCommand) (*AppendChunkCommandResponse, error) {
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
	if err := volume.GetChunkStorage().Append(ctx, readyChunk.ID(), knownSize, cmd.Body, cmd.N, domain.AppendChunkOptions{
		MinTailSlackLength: cmd.MinTailSlackLength,
	}); err != nil {
		return nil, WithChunkAndVolumeStatus(err, readyChunk, volume.GetStatusProvider().Get())
	}

	newReadyChunk := domain.NewReadyChunk(readyChunk.ID(), readyChunk.WriterKey(), readyChunk.CreatedAt(), h.nowFunc(), knownSize+cmd.N, version+1)
	if err := h.chunkRepository.Upsert(ctx, newReadyChunk); err != nil {
		return nil, WithChunkAndVolumeStatus(err, readyChunk, volume.GetStatusProvider().Get())
	}

	return &AppendChunkCommandResponse{
		Chunk:        newReadyChunk,
		VolumeStatus: volume.GetStatusProvider().Get(),
	}, nil
}
