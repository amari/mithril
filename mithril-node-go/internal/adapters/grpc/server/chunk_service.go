package adaptersgrpcserver

import (
	"bufio"
	"context"
	"fmt"
	"io"

	chunkv1 "github.com/amari/mithril/gen/go/proto/mithril/chunk/v1"
	applicationcommands "github.com/amari/mithril/mithril-node-go/internal/application/commands"
	applicationqueries "github.com/amari/mithril/mithril-node-go/internal/application/queries"
	"github.com/amari/mithril/mithril-node-go/internal/domain"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ChunkServiceServer struct {
	chunkv1.UnimplementedChunkServiceServer

	appendChunkCommandHandler      applicationcommands.AppendChunkCommandHandler
	createChunkCommandHandler      applicationcommands.CreateChunkCommandHandler
	deleteChunkCommandHandler      applicationcommands.DeleteChunkCommandHandler
	readChunkQueryHandler          applicationqueries.ReadChunkQueryHandler
	putChunkCommandHandler         applicationcommands.PutChunkCommandHandler
	shrinkChunkToFitCommandHandler applicationcommands.ShrinkChunkToFitCommandHandler
	statChunkQueryHandler          applicationqueries.StatChunkQueryHandler
}

var _ chunkv1.ChunkServiceServer = (*ChunkServiceServer)(nil)

func NewChunkServiceServer(
	appendChunkCommandHandler applicationcommands.AppendChunkCommandHandler,
	createChunkCommandHandler applicationcommands.CreateChunkCommandHandler,
	deleteChunkCommandHandler applicationcommands.DeleteChunkCommandHandler,
	readChunkQueryHandler applicationqueries.ReadChunkQueryHandler,
	putChunkCommandHandler applicationcommands.PutChunkCommandHandler,
	shrinkChunkToFitCommandHandler applicationcommands.ShrinkChunkToFitCommandHandler,
	statChunkQueryHandler applicationqueries.StatChunkQueryHandler,
) *ChunkServiceServer {
	return &ChunkServiceServer{
		appendChunkCommandHandler:      appendChunkCommandHandler,
		createChunkCommandHandler:      createChunkCommandHandler,
		deleteChunkCommandHandler:      deleteChunkCommandHandler,
		readChunkQueryHandler:          readChunkQueryHandler,
		putChunkCommandHandler:         putChunkCommandHandler,
		shrinkChunkToFitCommandHandler: shrinkChunkToFitCommandHandler,
		statChunkQueryHandler:          statChunkQueryHandler,
	}
}

func (s *ChunkServiceServer) CreateChunk(ctx context.Context, req *chunkv1.CreateChunkRequest) (*chunkv1.CreateChunkResponse, error) {
	resp, err := s.createChunkCommandHandler.Handle(ctx, &applicationcommands.CreateChunkCommand{
		WriterKey:          req.WriterKey,
		MinTailSlackLength: req.MinTailSlackLength,
	})
	if err != nil {
		return nil, StatusFromError(err).Err()
	}

	return &chunkv1.CreateChunkResponse{
		Chunk:  ChunkFromDomain(resp.Chunk),
		Volume: VolumeFromDomain(&resp.VolumeStatus),
	}, nil
}

func (s *ChunkServiceServer) PutChunk(stream grpc.ClientStreamingServer[chunkv1.PutChunkRequest, chunkv1.PutChunkResponse]) error {
	msg, err := stream.Recv()
	if err != nil {
		return err
	}
	header := msg.GetHeader()
	if header == nil {
		return status.Errorf(codes.InvalidArgument, "expected header")
	}

	body := &RecvReader{
		Recv: func() ([]byte, error) {
			msg, err := stream.Recv()
			if err != nil {
				return nil, err
			}

			dataMsg := msg.GetData()
			if dataMsg == nil {
				return nil, status.Errorf(codes.InvalidArgument, "expected data")
			}

			return dataMsg.GetData(), nil
		},
		expected: int(header.TotalDataLength),
	}

	resp, err := s.putChunkCommandHandler.Handle(stream.Context(), &applicationcommands.PutChunkCommand{
		WriterKey:          header.WriterKey,
		MinTailSlackLength: header.MinTailSlackLength,
		Body:               body,
		N:                  header.TotalDataLength,
	})
	if err != nil {
		return StatusFromError(err).Err()
	}

	if err := stream.SendAndClose(&chunkv1.PutChunkResponse{
		Chunk:  ChunkFromDomain(resp.Chunk),
		Volume: VolumeFromDomain(&resp.VolumeStatus),
	}); err != nil {
		return err
	}

	return nil
}

func (s *ChunkServiceServer) AppendChunk(stream grpc.ClientStreamingServer[chunkv1.AppendChunkRequest, chunkv1.AppendChunkResponse]) error {
	msg, err := stream.Recv()
	if err != nil {
		return err
	}
	header := msg.GetHeader()
	if header == nil {
		return status.Errorf(codes.InvalidArgument, "expected header")
	}

	body := &RecvReader{
		Recv: func() ([]byte, error) {
			msg, err := stream.Recv()
			if err != nil {
				return nil, err
			}

			dataMsg := msg.GetData()
			if dataMsg == nil {
				return nil, status.Errorf(codes.InvalidArgument, "expected data")
			}

			return dataMsg.GetData(), nil
		},
		expected: int(header.TotalDataLength),
	}

	resp, err := s.appendChunkCommandHandler.Handle(stream.Context(), &applicationcommands.AppendChunkCommand{
		WriterKey:          header.WriterKey,
		ExpectedVersion:    header.ExpectedVersion,
		MinTailSlackLength: header.MinTailSlackLength,
		Body:               body,
		N:                  header.TotalDataLength,
	})
	if err != nil {
		return StatusFromError(err).Err()
	}

	if err := stream.SendAndClose(&chunkv1.AppendChunkResponse{
		Chunk:  ChunkFromDomain(resp.Chunk),
		Volume: VolumeFromDomain(&resp.VolumeStatus),
	}); err != nil {
		return err
	}

	return nil
}

func (s *ChunkServiceServer) DeleteChunk(ctx context.Context, req *chunkv1.DeleteChunkRequest) (*chunkv1.DeleteChunkResponse, error) {
	resp, err := s.deleteChunkCommandHandler.Handle(ctx, &applicationcommands.DeleteChunkCommand{
		WriterKey: req.WriterKey,
	})

	if err != nil {
		return nil, StatusFromError(err).Err()
	}

	return &chunkv1.DeleteChunkResponse{
		Volume: VolumeFromDomain(&resp.VolumeStatus),
	}, nil
}

func (s *ChunkServiceServer) ShrinkChunk(ctx context.Context, req *chunkv1.ShrinkChunkRequest) (*chunkv1.ShrinkChunkResponse, error) {
	resp, err := s.shrinkChunkToFitCommandHandler.Handle(ctx, &applicationcommands.ShrinkChunkToFitCommand{
		WriterKey:          req.WriterKey,
		ExpectedVersion:    req.ExpectedVersion,
		MaxTailSlackLength: req.MaxTailSlackLength,
	})
	if err != nil {
		return nil, StatusFromError(err).Err()
	}

	return &chunkv1.ShrinkChunkResponse{
		Chunk:  ChunkFromDomain(resp.Chunk),
		Volume: VolumeFromDomain(&resp.VolumeStatus),
	}, nil
}

func (s *ChunkServiceServer) StatChunk(ctx context.Context, req *chunkv1.StatChunkRequest) (*chunkv1.StatChunkResponse, error) {
	resp, err := s.statChunkQueryHandler.Handle(ctx, &applicationqueries.StatChunkQuery{
		ChunkID: req.ChunkId,
	})
	if err != nil {
		return nil, StatusFromError(err).Err()
	}

	return &chunkv1.StatChunkResponse{
		Chunk:  ChunkFromDomain(resp.Chunk),
		Volume: VolumeFromDomain(&resp.VolumeStatus),
	}, nil
}

func (s *ChunkServiceServer) ReadChunk(req *chunkv1.ReadChunkRequest, stream grpc.ServerStreamingServer[chunkv1.ReadChunkResponse]) error {
	resp, err := s.readChunkQueryHandler.Handle(stream.Context(), &applicationqueries.ReadChunkQuery{
		ChunkID: req.ChunkId,
	})
	if err != nil {
		return StatusFromError(err).Err()
	}
	defer resp.ChunkHandle.Close()

	reader, err := resp.ChunkHandle.OpenRangeReader(stream.Context(), req.Offset, req.Length)
	if err != nil {
		return StatusFromError(err).Err()
	}
	defer reader.Close()

	knownSize, _ := resp.Chunk.Size()

	start := req.Offset
	end := req.Offset + req.Length

	if start > knownSize || end > knownSize || start < 0 || end < 0 {
		return StatusFromError(applicationcommands.WithChunkAndVolumeStatus(
			fmt.Errorf("bad range: %w", domain.ErrChunkInvalidRange),
			resp.Chunk,
			resp.VolumeStatus,
		)).Err()
	}

	err = stream.Send(&chunkv1.ReadChunkResponse{
		Response: &chunkv1.ReadChunkResponse_Header{
			Header: &chunkv1.ReadChunkResponseHeader{
				Chunk:           ChunkFromDomain(resp.Chunk),
				Volume:          VolumeFromDomain(&resp.VolumeStatus),
				TotalDataLength: min(req.Length, knownSize),
			},
		},
	})
	if err != nil {
		return err
	}

	w := bufio.NewWriter(WriterFunc(func(p []byte) (int, error) {
		volumeStatus := resp.GetVolumeStatus()

		if volumeStatus.Health == domain.VolumeFailed {
			return 0, domain.ErrVolumeFailed
		}

		err = stream.Send(&chunkv1.ReadChunkResponse{
			Response: &chunkv1.ReadChunkResponse_Data{
				Data: &chunkv1.ReadChunkResponseData{
					Data:   p,
					Volume: VolumeFromDomain(&volumeStatus),
				},
			},
		})
		if err != nil {
			return 0, err
		}

		return len(p), nil
	}))

	_, err = io.Copy(w, reader)
	if err != nil {
		return StatusFromError(err).Err()
	}

	return w.Flush()
}

type WriterFunc func([]byte) (int, error)

func (f WriterFunc) Write(p []byte) (n int, err error) {
	return f(p)
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
