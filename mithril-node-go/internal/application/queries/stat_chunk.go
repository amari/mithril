package applicationqueries

import (
	"context"

	applicationerrors "github.com/amari/mithril/mithril-node-go/internal/application/errors"
	applicationservices "github.com/amari/mithril/mithril-node-go/internal/application/services"
	"github.com/amari/mithril/mithril-node-go/internal/domain"
	"github.com/rs/zerolog"
)

type StatChunkQuery struct {
	ChunkID []byte
}

type StatChunkQueryResponse struct {
	Chunk        *domain.ReadyChunk
	VolumeStatus domain.VolumeStatus
}

type StatChunkQueryHandler interface {
	Handle(ctx context.Context, qry *StatChunkQuery) (*StatChunkQueryResponse, error)
}

type statChunkQueryHandler struct {
	chunkRepository domain.ChunkRepository
	volumeService   applicationservices.VolumeService
}

func NewStatChunkQueryHandler(chunkRepository domain.ChunkRepository, volumeService applicationservices.VolumeService) StatChunkQueryHandler {
	return &statChunkQueryHandler{
		chunkRepository: chunkRepository,
		volumeService:   volumeService,
	}
}

func (h *statChunkQueryHandler) Handle(ctx context.Context, qry *StatChunkQuery) (*StatChunkQueryResponse, error) {
	var chunkID domain.ChunkID
	if err := chunkID.UnmarshalBinary(qry.ChunkID); err != nil {
		return nil, err
	}

	chunk, err := h.chunkRepository.Get(ctx, chunkID)
	if err != nil {
		return nil, err
	}

	readyChunk, ok := chunk.(*domain.ReadyChunk)
	if !ok {
		return nil, domain.ErrChunkInvalidOperation
	}

	volume, err := h.volumeService.GetVolume(readyChunk.ID().VolumeID())
	if err != nil {
		return nil, applicationerrors.ContextErrorWithChunk(err, readyChunk)
	}

	logger := zerolog.Ctx(ctx)
	if logger != nil {
		newLogger := logger.With().Fields(volume.GetStructuredLoggingFieldsProvider().Get()).Logger()
		ctx = newLogger.WithContext(ctx)
	}

	return &StatChunkQueryResponse{
		Chunk:        readyChunk,
		VolumeStatus: volume.GetStatusProvider().Get(),
	}, nil
}
