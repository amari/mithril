package grpc

import (
	"bufio"
	"context"
	"io"

	"github.com/amari/mithril/chunk-node/service/chunk/command"
	"github.com/amari/mithril/chunk-node/service/chunk/query"
	chunkstorev1 "github.com/amari/mithril/gen/go/proto/chunkstore/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ChunkStoreChunkServiceServer struct {
	chunkstorev1.UnimplementedChunkServiceServer

	createChunk *command.CreateChunkHandler
	putChunk    *command.PutChunkHandler
	appendChunk *command.AppendChunkHandler
	deleteChunk *command.DeleteChunkHandler
	shrinkChunk *command.ShrinkChunkHandler

	readChunk *query.ReadChunkHandler
	statChunk *query.StatChunkHandler
}

var _ chunkstorev1.ChunkServiceServer = (*ChunkStoreChunkServiceServer)(nil)

func NewChunkStoreChunkServiceServer(
	createChunk *command.CreateChunkHandler,
	putChunk *command.PutChunkHandler,
	appendChunk *command.AppendChunkHandler,
	deleteChunk *command.DeleteChunkHandler,
	shrinkChunk *command.ShrinkChunkHandler,
	readChunk *query.ReadChunkHandler,
	statChunk *query.StatChunkHandler,
) *ChunkStoreChunkServiceServer {
	return &ChunkStoreChunkServiceServer{
		createChunk: createChunk,
		putChunk:    putChunk,
		appendChunk: appendChunk,
		deleteChunk: deleteChunk,
		shrinkChunk: shrinkChunk,
		readChunk:   readChunk,
		statChunk:   statChunk,
	}
}

func (srv *ChunkStoreChunkServiceServer) AppendToChunk(ss grpc.ClientStreamingServer[chunkstorev1.AppendToChunkRequest, chunkstorev1.AppendToChunkResponse]) error {
	ctx := ss.Context()

	msg, err := ss.Recv()
	if err != nil {
		return err
	}

	header := msg.GetHeader()
	if header == nil {
		return status.Errorf(codes.InvalidArgument, "expected header")
	}

	minTailSlackSize := uint64(0)
	if header.MinTailSlackSize != nil {
		minTailSlackSize = *header.MinTailSlackSize
	}

	stream := &bodyAppendStream{
		recvFunc: func() ([]byte, error) {
			msg, err := ss.Recv()
			if err != nil {
				return nil, err
			}

			bodyFragment := msg.GetBodyFragment()
			if bodyFragment == nil {
				return nil, status.Errorf(codes.InvalidArgument, "expected body fragment")
			}

			return bodyFragment.GetData(), nil
		},
		total: int(header.BodySize),
	}

	output, err := srv.appendChunk.HandleAppendChunk(ctx, &command.AppendChunkInput{
		WriteKey:         header.WriteKey,
		ExpectedVersion:  uint64(header.ExpectedVersion),
		MinTailSlackSize: int64(minTailSlackSize),
		Body:             stream,
		BodySize:         header.BodySize,
	})

	if err != nil {
		return status.Errorf(codes.Internal, "failed to append chunk: %v", err)
	}

	return ss.SendAndClose(&chunkstorev1.AppendToChunkResponse{
		Chunk: &chunkstorev1.Chunk{
			Id:      output.Chunk.ID[:],
			Size:    uint64(output.Chunk.Size),
			Version: output.Chunk.Version,
		},
	})
}

type bodyAppendStream struct {
	recvFunc func() ([]byte, error)
	total    int

	buf  []byte
	read int
}

func (s *bodyAppendStream) RecvBodyFragment() ([]byte, error) {
	body, err := s.recvFunc()
	if err != nil {
		return nil, err
	}
	if body == nil {
		return nil, status.Errorf(codes.InvalidArgument, "expected body")
	}

	return body, nil
}

func (s *bodyAppendStream) Read(p []byte) (n int, err error) {
	if s.read >= s.total {
		return 0, io.EOF
	}

	remaining := s.total - s.read
	if len(p) > remaining {
		p = p[:remaining]
	}

	buf := s.buf
	read := s.read

	if len(s.buf) > 0 {
		n = copy(p, buf)
		buf = buf[n:]
		p = p[n:]

		read += n

		if len(p) == 0 {
			s.buf = buf
			s.read = read

			return n, nil
		}
	}

	bodyFragment, err := s.RecvBodyFragment()
	if err != nil {
		if n > 0 {
			s.buf = buf
			s.read = read

			return n, nil
		}

		return 0, err
	}

	if len(bodyFragment) == 0 {
		s.buf = buf
		s.read = read

		return n, nil
	}

	m := copy(p, bodyFragment)
	bodyFragment = bodyFragment[m:]
	p = p[m:]

	n += m
	read += m

	if len(bodyFragment) > 0 {
		s.buf = bodyFragment
	} else {
		s.buf = nil
	}

	s.buf = buf
	s.read = read

	if s.read >= s.total {
		return n, io.EOF
	}

	return n, nil
}

func (srv *ChunkStoreChunkServiceServer) CreateChunk(ctx context.Context, request *chunkstorev1.CreateChunkRequest) (*chunkstorev1.CreateChunkResponse, error) {
	minTailSlackSize := uint64(0)
	if request.MinTailSlackSize != nil {
		minTailSlackSize = *request.MinTailSlackSize
	}

	output, err := srv.createChunk.HandleCreateChunk(ctx, &command.CreateChunkInput{
		WriteKey:         request.WriteKey,
		MinTailSlackSize: int64(minTailSlackSize),
	})
	if err != nil {
		// FIXME: map error
		return nil, err
	}

	return &chunkstorev1.CreateChunkResponse{
		Chunk: &chunkstorev1.Chunk{
			Id:      output.Chunk.ID[:],
			Size:    uint64(output.Chunk.Size),
			Version: output.Chunk.Version,
		},
	}, nil
}

func (srv *ChunkStoreChunkServiceServer) DeleteChunk(ctx context.Context, request *chunkstorev1.DeleteChunkRequest) (*chunkstorev1.DeleteChunkResponse, error) {
	_, err := srv.deleteChunk.HandleDeleteChunk(ctx, &command.DeleteChunkInput{
		WriteKey: request.WriteKey,
	})
	if err != nil {
		// FIXME: map error
		return nil, err
	}

	return &chunkstorev1.DeleteChunkResponse{}, nil
}

func (srv *ChunkStoreChunkServiceServer) PutChunk(ss grpc.ClientStreamingServer[chunkstorev1.PutChunkRequest, chunkstorev1.PutChunkResponse]) error {
	ctx := ss.Context()

	msg, err := ss.Recv()
	if err != nil {
		return err
	}

	header := msg.GetHeader()
	if header == nil {
		return status.Errorf(codes.InvalidArgument, "expected header")
	}

	minTailSlackSize := uint64(0)
	if header.MinTailSlackSize != nil {
		minTailSlackSize = *header.MinTailSlackSize
	}

	stream := &bodyAppendStream{
		recvFunc: func() ([]byte, error) {
			msg, err := ss.Recv()
			if err != nil {
				return nil, err
			}

			bodyFragment := msg.GetBodyFragment()
			if bodyFragment == nil {
				return nil, status.Errorf(codes.InvalidArgument, "expected body fragment")
			}

			return bodyFragment.GetData(), nil
		},
		total: int(header.BodySize),
	}

	output, err := srv.putChunk.HandlePutChunk(ctx, &command.PutChunkInput{
		WriteKey:         header.WriteKey,
		MinTailSlackSize: int64(minTailSlackSize),
		Body:             stream,
		BodySize:         header.BodySize,
	})

	if err != nil {
		return status.Errorf(codes.Internal, "failed to append chunk: %v", err)
	}

	return ss.SendAndClose(&chunkstorev1.PutChunkResponse{
		Chunk: &chunkstorev1.Chunk{
			Id:      output.Chunk.ID[:],
			Size:    uint64(output.Chunk.Size),
			Version: output.Chunk.Version,
		},
	})
}

func (srv *ChunkStoreChunkServiceServer) ShrinkChunkTailSlack(ctx context.Context, request *chunkstorev1.ShrinkChunkTailSlackRequest) (*chunkstorev1.ShrinkChunkTailSlackResponse, error) {
	output, err := srv.shrinkChunk.HandleShrinkChunk(ctx, &command.ShrinkChunkInput{
		WriteKey:         request.WriteKey,
		ExpectedVersion:  uint64(request.ExpectedVersion),
		MaxTailSlackSize: request.MaxTailSlackSize,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to shrink chunk tail slack: %v", err)
	}

	return &chunkstorev1.ShrinkChunkTailSlackResponse{
		Chunk: &chunkstorev1.Chunk{
			Id:      output.Chunk.ID[:],
			Size:    uint64(output.Chunk.Size),
			Version: output.Chunk.Version,
		},
	}, nil
}

func (srv *ChunkStoreChunkServiceServer) ReadChunk(request *chunkstorev1.ReadChunkRequest, ss grpc.ServerStreamingServer[chunkstorev1.ReadChunkResponse]) error {
	output, err := srv.readChunk.HandleReadChunk(ss.Context(), &query.ReadChunkInput{
		ChunkID:   request.ChunkId,
		WriterKey: request.WriteKey,
	})
	if err != nil {
		return status.Errorf(codes.Internal, "failed to read chunk: %v", err)
	}
	defer output.Handle.Close()

	// validate ranges
	totalLength := int64(0)
	for _, operation := range request.Ranges {
		if operation.Length < 0 {
			return status.Errorf(codes.InvalidArgument, "invalid operation length: %d", operation.Length)
		}
		if operation.Offset < 0 || operation.Offset+operation.Length > output.Chunk.Size {
			return status.Errorf(codes.InvalidArgument, "invalid operation offset/length: offset=%d, length=%d", operation.Offset, operation.Length)
		}
		totalLength += operation.Length
	}

	// write the header
	err = ss.Send(&chunkstorev1.ReadChunkResponse{
		Frame: &chunkstorev1.ReadChunkResponse_Header_{
			Header: &chunkstorev1.ReadChunkResponse_Header{
				Chunk: &chunkstorev1.Chunk{
					Id:      output.Chunk.ID[:],
					Size:    uint64(output.Chunk.Size),
					Version: output.Chunk.Version,
				},
			},
		},
	})
	if err != nil {
		return status.Errorf(codes.Internal, "failed to send chunk header: %v", err)
	}

	w := bufio.NewWriter(writerFunc(func(p []byte) (int, error) {
		err := ss.Send(&chunkstorev1.ReadChunkResponse{
			Frame: &chunkstorev1.ReadChunkResponse_BodyFragment_{
				BodyFragment: &chunkstorev1.ReadChunkResponse_BodyFragment{
					Data: p,
				},
			},
		})
		if err != nil {
			return 0, status.Errorf(codes.Internal, "failed to send chunk body fragment: %v", err)
		}

		return len(p), nil
	}))

	for _, r := range request.Ranges {
		chunkReader, err := output.Handle.NewRangeReader(ss.Context(), r.Offset, r.Length)

		if err != nil {
			return status.Errorf(codes.Internal, "failed to get chunk reader: %v", err)
		}
		defer chunkReader.Close()

		_, err = io.Copy(w, chunkReader)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to read chunk data: %v", err)
		}

		// flush if buffer exceeds 4MiB
		if w.Buffered() >= 4*1024*1024 {
			if err := w.Flush(); err != nil {
				return err
			}
		}
	}

	return w.Flush()
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (n int, err error) {
	return f(p)
}

func (srv *ChunkStoreChunkServiceServer) StatChunk(ctx context.Context, request *chunkstorev1.StatChunkRequest) (*chunkstorev1.StatChunkResponse, error) {
	output, err := srv.statChunk.HandleStatChunk(ctx, &query.StatChunkInput{
		ChunkID:   request.ChunkId,
		WriterKey: request.WriteKey,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to stat chunk: %v", err)
	}

	return &chunkstorev1.StatChunkResponse{
		Chunk: &chunkstorev1.Chunk{
			Id:      output.Chunk.ID[:],
			Size:    uint64(output.Chunk.Size),
			Version: output.Chunk.Version,
		},
	}, nil
}
