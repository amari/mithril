package applicationcommands

import (
	"context"
	"io"
	"time"

	applicationservices "github.com/amari/mithril/mithril-node-go/internal/application/services"
	"github.com/amari/mithril/mithril-node-go/internal/domain"
	"github.com/rs/zerolog"
)

type AppendFromChunkCommand struct {
	WriterKey          []byte
	ExpectedVersion    uint64
	MinTailSlackLength int64

	OtherChunkID         []byte
	OtherChunkRangeStart int64
	OtherChunkRangeEnd   int64
}

type AppendFromChunkCommandResponse struct {
	Chunk        *domain.ReadyChunk
	VolumeStatus domain.VolumeStatus
}

type AppendFromChunkCommandHandler interface {
	Handle(ctx context.Context, cmd *AppendFromChunkCommand) (*AppendFromChunkCommandResponse, error)
}

type appendFromChunkCommandHandler struct {
	nowFunc                              func() time.Time
	chunkRepository                      domain.ChunkRepository
	remotePeerChunkServiceClientProvider applicationservices.RemotePeerChunkServiceClientProvider
	volumeService                        applicationservices.VolumeService
}

func NewAppendFromChunkCommandHandler(
	nowFunc func() time.Time,
	chunkRepository domain.ChunkRepository,
	remotePeerChunkServiceClientProvider applicationservices.RemotePeerChunkServiceClientProvider,
	volumeService applicationservices.VolumeService,
) AppendFromChunkCommandHandler {
	return &appendFromChunkCommandHandler{
		nowFunc:                              nowFunc,
		chunkRepository:                      chunkRepository,
		remotePeerChunkServiceClientProvider: remotePeerChunkServiceClientProvider,
		volumeService:                        volumeService,
	}
}

func (h *appendFromChunkCommandHandler) Handle(ctx context.Context, cmd *AppendFromChunkCommand) (*AppendFromChunkCommandResponse, error) {
	var otherChunkID domain.ChunkID
	if err := otherChunkID.UnmarshalBinary(cmd.OtherChunkID); err != nil {
		return nil, domain.ErrChunkInvalidID
	}
	otherNodeID := otherChunkID.NodeID()

	chunk, err := h.chunkRepository.GetWithWriterKey(ctx, cmd.WriterKey)
	if err != nil {
		return nil, err
	}

	readyChunk, ok := chunk.(*domain.ReadyChunk)
	if !ok {
		return nil, domain.ErrChunkInvalidOperation
	}

	volume, err := h.volumeService.GetVolume(readyChunk.ID().VolumeID())
	if err != nil {
		return nil, WithChunk(err, readyChunk)
	}

	version, ok := readyChunk.Version()
	if !ok || version != cmd.ExpectedVersion {
		return nil, WithChunkAndVolumeStatus(domain.ErrChunkInvalidVersion, readyChunk, volume.GetStatusProvider().Get())
	}

	logger := zerolog.Ctx(ctx)
	if logger != nil {
		newLogger := logger.With().Fields(volume.GetStructuredLoggingFieldsProvider().Get()).Logger()
		ctx = newLogger.WithContext(ctx)
	}

	client, err := h.remotePeerChunkServiceClientProvider.GetRemotePeerChunkServiceClient(otherNodeID)
	if err != nil {
		return nil, WithChunkAndVolumeStatus(err, readyChunk, volume.GetStatusProvider().Get())
	}

	reader, err := client.OpenRangeReader(ctx, otherChunkID, cmd.OtherChunkRangeStart, cmd.OtherChunkRangeEnd-cmd.OtherChunkRangeStart)
	if err != nil {
		return nil, WithChunkAndVolumeStatus(err, readyChunk, volume.GetStatusProvider().Get())
	}
	defer reader.Close()

	knownSize, _ := readyChunk.Size()
	if err := volume.GetChunkStorage().Append(ctx, readyChunk.ID(), knownSize, reader, cmd.OtherChunkRangeEnd-cmd.OtherChunkRangeStart, domain.AppendChunkOptions{
		MinTailSlackLength: cmd.MinTailSlackLength,
	}); err != nil {
		return nil, WithChunkAndVolumeStatus(err, readyChunk, volume.GetStatusProvider().Get())
	}

	newReadyChunk := domain.NewReadyChunk(readyChunk.ID(), readyChunk.WriterKey(), readyChunk.CreatedAt(), h.nowFunc(), knownSize+cmd.OtherChunkRangeEnd-cmd.OtherChunkRangeStart, version+1)
	if err := h.chunkRepository.Upsert(ctx, newReadyChunk); err != nil {
		return nil, WithChunkAndVolumeStatus(err, readyChunk, volume.GetStatusProvider().Get())
	}

	return &AppendFromChunkCommandResponse{
		Chunk:        newReadyChunk,
		VolumeStatus: volume.GetStatusProvider().Get(),
	}, nil
}

type RecvReader struct {
	Recv     func() ([]byte, error)
	expected int
	read     int
	buf      []byte
}

func (rr *RecvReader) Read(p []byte) (n int, err error) {
	if rr.read >= rr.expected {
		return 0, io.EOF
	}

	expected := rr.expected - rr.read
	if len(p) > expected {
		p = p[:expected]
	}

	buf := rr.buf
	read := rr.read

	if len(rr.buf) > 0 {
		n = copy(p, buf)
		buf = buf[n:]
		p = p[n:]

		read += n

		if len(p) == 0 {
			rr.buf = buf
			rr.read = read

			return n, nil
		}
	}

	data, err := rr.Recv()
	if err != nil {
		if n > 0 {
			rr.buf = buf
			rr.read = read

			return n, nil
		}

		return 0, err
	}

	if len(data) == 0 {
		rr.buf = buf
		rr.read = read

		return n, nil
	}

	m := copy(p, data)
	data = data[m:]
	p = p[m:]

	n += m
	read += m

	if len(data) > 0 {
		buf = data
	} else {
		buf = nil
	}

	rr.buf = buf
	rr.read = read

	if rr.read >= rr.expected {
		return n, io.EOF
	}

	return n, nil
}
