package applicationqueries

import (
	"context"

	applicationerrors "github.com/amari/mithril/mithril-node-go/internal/application/errors"
	applicationservices "github.com/amari/mithril/mithril-node-go/internal/application/services"
	"github.com/amari/mithril/mithril-node-go/internal/domain"
	"github.com/rs/zerolog"
)

type ReadChunkQuery struct {
	ChunkID []byte
}

type ReadChunkQueryResponse struct {
	Chunk        *domain.ReadyChunk
	VolumeStatus domain.VolumeStatus

	ChunkHandle     domain.ChunkHandle
	GetVolumeStatus func() domain.VolumeStatus
}

type ReadChunkQueryHandler interface {
	Handle(ctx context.Context, qry *ReadChunkQuery) (*ReadChunkQueryResponse, error)
}

type readChunkQueryHandler struct {
	chunkRepository domain.ChunkRepository
	volumeService   applicationservices.VolumeService
}

func NewReadChunkQueryHandler(chunkRepository domain.ChunkRepository, volumeService applicationservices.VolumeService) ReadChunkQueryHandler {
	return &readChunkQueryHandler{
		chunkRepository: chunkRepository,
		volumeService:   volumeService,
	}
}

func (h *readChunkQueryHandler) Handle(ctx context.Context, qry *ReadChunkQuery) (*ReadChunkQueryResponse, error) {
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
		return nil, applicationerrors.WithChunk(err, readyChunk)
	}

	logger := zerolog.Ctx(ctx)
	if logger != nil {
		newLogger := logger.With().
			Str("chunk.id", chunkID.String()).
			Uint32("chunk.node.id", uint32(volume.Info().Node)).
			Uint16("chunk.volume.id", uint16(volume.Info().ID)).
			Fields(volume.GetStructuredLoggingFieldsProvider().Get()).
			Logger()
		ctx = newLogger.WithContext(ctx)
	}

	handle, err := volume.GetChunkStorage().Open(ctx, chunkID)
	if err != nil {
		return nil, applicationerrors.WithChunkAndVolumeStatus(err, readyChunk, volume.GetStatusProvider().Get())
	}

	return &ReadChunkQueryResponse{
		Chunk:           readyChunk,
		VolumeStatus:    volume.GetStatusProvider().Get(),
		ChunkHandle:     handle,
		GetVolumeStatus: volume.GetStatusProvider().Get,
	}, nil
}
