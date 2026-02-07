package grpc

import (
	"bufio"
	"context"
	"fmt"
	"io"

	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/errors"
	chunkstoreerrors "github.com/amari/mithril/chunk-node/errors"
	"github.com/amari/mithril/chunk-node/service/chunk/command"
	"github.com/amari/mithril/chunk-node/service/chunk/query"
	chunkv1 "github.com/amari/mithril/gen/go/proto/mithril/chunk/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ChunkServiceServer struct {
	appendChunk     *command.AppendChunkHandler
	appendFromChunk *command.AppendFromChunkHandler
	createChunk     *command.CreateChunkHandler
	deleteChunk     *command.DeleteChunkHandler
	putChunk        *command.PutChunkHandler
	shrinkChunk     *command.ShrinkChunkHandler

	readChunk *query.ReadChunkHandler
	statChunk *query.StatChunkHandler

	chunkv1.UnimplementedChunkServiceServer
}

var _ chunkv1.ChunkServiceServer = (*ChunkServiceServer)(nil)

func NewChunkServiceServer(
	appendChunk *command.AppendChunkHandler,
	appendFromChunk *command.AppendFromChunkHandler,
	createChunk *command.CreateChunkHandler,
	deleteChunk *command.DeleteChunkHandler,
	putChunk *command.PutChunkHandler,
	readChunk *query.ReadChunkHandler,
	shrinkChunk *command.ShrinkChunkHandler,
	statChunk *query.StatChunkHandler,
) *ChunkServiceServer {
	return &ChunkServiceServer{
		appendChunk:     appendChunk,
		appendFromChunk: appendFromChunk,
		createChunk:     createChunk,
		deleteChunk:     deleteChunk,
		putChunk:        putChunk,
		readChunk:       readChunk,
		shrinkChunk:     shrinkChunk,
		statChunk:       statChunk,
	}
}

func (srv *ChunkServiceServer) AppendChunk(ss grpc.ClientStreamingServer[chunkv1.AppendChunkRequest, chunkv1.AppendChunkResponse]) error {
	ctx := ss.Context()

	msg, err := ss.Recv()
	if err != nil {
		return mapErrorToStatus(err, domain.VolumeStateUnknown)
	}

	header := msg.GetHeader()
	if header == nil {
		return status.Errorf(codes.InvalidArgument, "expected header")
	}

	minTailSlackLength := header.MinTailSlackLength

	stream := &bodyAppendStream{
		recvFunc: func() ([]byte, error) {
			msg, err := ss.Recv()
			if err != nil {
				return nil, err
			}

			bodyFragment := msg.GetData()
			if bodyFragment == nil {
				return nil, fmt.Errorf("%w: expected body fragment", chunkstoreerrors.ErrWriteSizeMismatch)
			}

			return bodyFragment.GetData(), nil
		},
		total: int(header.TotalDataLength),
	}

	output, err := srv.appendChunk.HandleAppendChunk(ctx, &command.AppendChunkInput{
		WriteKey:         header.WriterKey,
		ExpectedVersion:  header.ExpectedVersion,
		MinTailSlackSize: minTailSlackLength,
		Body:             stream,
		BodySize:         header.TotalDataLength,
	})

	if err != nil {
		return mapErrorToStatus(err, domain.VolumeStateOK)
	}

	return ss.SendAndClose(&chunkv1.AppendChunkResponse{
		Chunk: &chunkv1.Chunk{
			Id:      output.Chunk.ID[:],
			Size:    output.Chunk.Size,
			Version: output.Chunk.Version,
		},
		Volume: &chunkv1.Volume{
			State: chunkv1.Volume_STATE_OK,
		},
	})
}

func (srv *ChunkServiceServer) AppendFromChunk(ctx context.Context, r *chunkv1.AppendFromChunkRequest) (*chunkv1.AppendFromChunkResponse, error) {
	output, err := srv.appendFromChunk.HandleAppendFromChunk(ctx, &command.AppendFromChunkInput{
		WriteKey:          r.WriterKey,
		ExpectedVersion:   r.ExpectedVersion,
		MinTailSlackSize:  r.MinTailSlackLength,
		RemoteChunkID:     r.RemoteChunkId,
		RemoteChunkOffset: r.RemoteChunkOffset,
		RemoteChunkLength: r.RemoteChunkLength,
	})
	if err != nil {
		return nil, mapErrorToStatus(err, domain.VolumeStateOK)
	}

	return &chunkv1.AppendFromChunkResponse{
		Chunk: &chunkv1.Chunk{
			Id:      output.Chunk.ID[:],
			Size:    output.Chunk.Size,
			Version: output.Chunk.Version,
		},
		Volume: &chunkv1.Volume{
			State: chunkv1.Volume_STATE_OK,
		},
	}, nil
}

func (srv *ChunkServiceServer) CreateChunk(ctx context.Context, r *chunkv1.CreateChunkRequest) (*chunkv1.CreateChunkResponse, error) {
	minTailSlackSize := r.MinTailSlackLength

	output, err := srv.createChunk.HandleCreateChunk(ctx, &command.CreateChunkInput{
		WriteKey:         r.WriterKey,
		MinTailSlackSize: minTailSlackSize,
	})
	if err != nil {
		return nil, mapErrorToStatus(err, domain.VolumeStateOK)
	}

	return &chunkv1.CreateChunkResponse{
		Chunk: &chunkv1.Chunk{
			Id:      output.Chunk.ID[:],
			Size:    output.Chunk.Size,
			Version: output.Chunk.Version,
		},
		Volume: &chunkv1.Volume{
			State: chunkv1.Volume_STATE_OK,
		},
	}, nil
}

func (srv *ChunkServiceServer) DeleteChunk(ctx context.Context, r *chunkv1.DeleteChunkRequest) (*chunkv1.DeleteChunkResponse, error) {
	output, err := srv.deleteChunk.HandleDeleteChunk(ctx, &command.DeleteChunkInput{
		WriteKey: r.WriterKey,
	})
	if err != nil {
		return nil, mapErrorToStatus(err, domain.VolumeStateOK)
	}

	return &chunkv1.DeleteChunkResponse{
		Chunk: &chunkv1.Chunk{
			Id:      output.Chunk.ID[:],
			Size:    0,
			Version: 0,
		},
		Volume: &chunkv1.Volume{
			State: chunkv1.Volume_STATE_OK,
		},
	}, nil
}

func (srv *ChunkServiceServer) PutChunk(ss grpc.ClientStreamingServer[chunkv1.PutChunkRequest, chunkv1.PutChunkResponse]) error {
	ctx := ss.Context()

	msg, err := ss.Recv()
	if err != nil {
		return err
	}

	header := msg.GetHeader()
	if header == nil {
		return status.Errorf(codes.InvalidArgument, "expected header")
	}

	minTailSlackSize := header.MinTailSlackLength

	stream := &bodyAppendStream{
		recvFunc: func() ([]byte, error) {
			msg, err := ss.Recv()
			if err != nil {
				return nil, err
			}

			bodyFragment := msg.GetData()
			if bodyFragment == nil {
				return nil, fmt.Errorf("%w: expected body fragment", chunkstoreerrors.ErrWriteSizeMismatch)
			}

			return bodyFragment.GetData(), nil
		},
		total: int(header.TotalDataLength),
	}

	output, err := srv.putChunk.HandlePutChunk(ctx, &command.PutChunkInput{
		WriteKey:         header.WriterKey,
		MinTailSlackSize: minTailSlackSize,
		Body:             stream,
		BodySize:         header.TotalDataLength,
	})

	if err != nil {
		return mapErrorToStatus(&chunkstoreerrors.ChunkError{
			Cause: chunkstoreerrors.ErrOffsetOutOfBounds,
			Chunk: &errors.Chunk{
				ID:      output.Chunk.ID[:],
				Version: output.Chunk.Version,
				Size:    output.Chunk.Size,
			},
		}, domain.VolumeStateOK)
	}

	return ss.SendAndClose(&chunkv1.PutChunkResponse{
		Chunk: &chunkv1.Chunk{
			Id:      output.Chunk.ID[:],
			Size:    output.Chunk.Size,
			Version: output.Chunk.Version,
		},
		Volume: &chunkv1.Volume{
			State: chunkv1.Volume_STATE_OK,
		},
	})
}

func (srv *ChunkServiceServer) ReadChunk(r *chunkv1.ReadChunkRequest, ss grpc.ServerStreamingServer[chunkv1.ReadChunkResponse]) error {
	output, err := srv.readChunk.HandleReadChunk(ss.Context(), &query.ReadChunkInput{
		ChunkID: r.ChunkId,
	})
	if err != nil {
		return mapErrorToStatus(err, domain.VolumeStateOK)
	}
	defer output.Handle.Close()

	// FIXME: we should be doing this in the handler, but we need to revise the chunk store OpenChunk function to accept a logical chunk size.
	start := r.Offset
	end := r.Offset + r.Length

	if start > output.Chunk.Size || end > output.Chunk.Size || start < 0 || end < 0 || start > end {
		return mapErrorToStatus(&chunkstoreerrors.ChunkError{
			Cause: chunkstoreerrors.ErrOffsetOutOfBounds,
			Chunk: &errors.Chunk{
				ID:      output.Chunk.ID[:],
				Version: output.Chunk.Version,
				Size:    output.Chunk.Size,
			},
		}, domain.VolumeStateOK)
	}

	// write the header
	err = ss.Send(&chunkv1.ReadChunkResponse{
		Response: &chunkv1.ReadChunkResponse_Header{
			Header: &chunkv1.ReadChunkResponseHeader{
				Chunk: &chunkv1.Chunk{
					Id:      output.Chunk.ID[:],
					Size:    output.Chunk.Size,
					Version: output.Chunk.Version,
				},
				Volume: &chunkv1.Volume{
					State: chunkv1.Volume_STATE_OK,
				},
				TotalDataLength: end - start,
			},
		},
	})
	if err != nil {
		return mapErrorToStatus(&chunkstoreerrors.ChunkError{
			Cause: err,
			Chunk: &errors.Chunk{
				ID:      output.Chunk.ID[:],
				Version: output.Chunk.Version,
				Size:    output.Chunk.Size,
			},
		}, domain.VolumeStateOK)
	}

	w := bufio.NewWriter(writerFunc(func(p []byte) (int, error) {
		volumeHealth, err := output.CheckVolumeHealthFunc()
		if err != nil {
			return 0, err
		}
		if volumeHealth.State == domain.VolumeStateFailed {
			return 0, &chunkstoreerrors.VolumeError{
				Cause: chunkstoreerrors.ErrVolumeFailed,
				Volume: &errors.Volume{
					State: chunkstoreerrors.VolumeStateFailed,
				},
			}
		}

		// send data chunk
		err = ss.Send(&chunkv1.ReadChunkResponse{
			Response: &chunkv1.ReadChunkResponse_Data{
				Data: &chunkv1.ReadChunkResponseData{
					Data: p,
					Volume: &chunkv1.Volume{
						State: mapVolumeState(volumeHealth.State),
					},
				},
			},
		})
		if err != nil {
			return 0, err
		}

		return len(p), nil
	}))

	chunkReader, err := output.Handle.NewRangeReader(ss.Context(), start, end-start)

	if err != nil {
		return mapErrorToStatus(&chunkstoreerrors.ChunkError{
			Cause: err,
			Chunk: &errors.Chunk{
				ID:      output.Chunk.ID[:],
				Version: output.Chunk.Version,
				Size:    output.Chunk.Size,
			},
		}, domain.VolumeStateOK)
	}
	defer chunkReader.Close()

	_, err = io.Copy(w, chunkReader)
	if err != nil {
		return mapErrorToStatus(&chunkstoreerrors.ChunkError{
			Cause: err,
			Chunk: &errors.Chunk{
				ID:      output.Chunk.ID[:],
				Version: output.Chunk.Version,
				Size:    output.Chunk.Size,
			},
		}, domain.VolumeStateOK)
	}

	return w.Flush()
}

func (srv *ChunkServiceServer) ShrinkChunk(ctx context.Context, r *chunkv1.ShrinkChunkRequest) (*chunkv1.ShrinkChunkResponse, error) {
	output, err := srv.shrinkChunk.HandleShrinkChunk(ctx, &command.ShrinkChunkInput{
		WriteKey:         r.WriterKey,
		ExpectedVersion:  r.ExpectedVersion,
		MaxTailSlackSize: r.MaxTailSlackLength,
	})
	if err != nil {
		return nil, mapErrorToStatus(err, domain.VolumeStateOK)
	}

	return &chunkv1.ShrinkChunkResponse{
		Chunk: &chunkv1.Chunk{
			Id:      output.Chunk.ID[:],
			Size:    output.Chunk.Size,
			Version: output.Chunk.Version,
		},
		Volume: &chunkv1.Volume{
			State: chunkv1.Volume_STATE_OK,
		},
	}, nil
}

func (srv *ChunkServiceServer) StatChunk(ctx context.Context, r *chunkv1.StatChunkRequest) (*chunkv1.StatChunkResponse, error) {
	output, err := srv.statChunk.HandleStatChunk(ctx, &query.StatChunkInput{
		ChunkID: r.ChunkId,
	})
	if err != nil {
		return nil, mapErrorToStatus(err, domain.VolumeStateOK)
	}

	return &chunkv1.StatChunkResponse{
		Chunk: &chunkv1.Chunk{
			Id:      output.Chunk.ID[:],
			Size:    output.Chunk.Size,
			Version: output.Chunk.Version,
		},
		Volume: &chunkv1.Volume{
			State: mapVolumeState(output.VolumeHealth.State),
		},
	}, nil
}
