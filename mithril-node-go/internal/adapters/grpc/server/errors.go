package adaptersgrpcserver

import (
	"errors"

	chunkv1 "github.com/amari/mithril/gen/go/proto/mithril/chunk/v1"
	applicationerrors "github.com/amari/mithril/mithril-node-go/internal/application/errors"
	"github.com/amari/mithril/mithril-node-go/internal/domain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func StatusFromError(err error) *status.Status {
	if err == nil {
		return status.New(codes.OK, "")
	}

	st := status.New(codes.Internal, err.Error())
	code := chunkv1.ErrorCode_ERROR_CODE_INTERNAL

	switch {
	case errors.Is(err, domain.ErrChunkNotFound):
		st = status.New(codes.NotFound, err.Error())
		code = chunkv1.ErrorCode_ERROR_CODE_CHUNK_NOT_FOUND
	case errors.Is(err, domain.ErrChunkAlreadyExists):
		st = status.New(codes.AlreadyExists, err.Error())
		code = chunkv1.ErrorCode_ERROR_CODE_CHUNK_ALREADY_EXISTS
	case errors.Is(err, domain.ErrChunkCorrupted):
		st = status.New(codes.DataLoss, err.Error())
		code = chunkv1.ErrorCode_ERROR_CODE_CHUNK_CORRUPTED
	case errors.Is(err, domain.ErrChunkWriteIncomplete):
		st = status.New(codes.Unavailable, err.Error())
		code = chunkv1.ErrorCode_ERROR_CODE_CHUNK_WRITE_INCOMPLETE
	case errors.Is(err, domain.ErrChunkInvalidOperation):
		st = status.New(codes.FailedPrecondition, err.Error())
		code = chunkv1.ErrorCode_ERROR_CODE_CHUNK_INVALID_OPERATION
	case errors.Is(err, domain.ErrChunkInvalidVersion):
		st = status.New(codes.FailedPrecondition, err.Error())
		code = chunkv1.ErrorCode_ERROR_CODE_CHUNK_INVALID_VERSION
	case errors.Is(err, domain.ErrChunkInvalidRange):
		st = status.New(codes.InvalidArgument, err.Error())
		code = chunkv1.ErrorCode_ERROR_CODE_CHUNK_INVALID_RANGE
	case errors.Is(err, domain.ErrChunkInvalidID):
		st = status.New(codes.InvalidArgument, err.Error())
		code = chunkv1.ErrorCode_ERROR_CODE_CHUNK_INVALID_ID
	case errors.Is(err, domain.ErrClockNotMonotonic):
		st = status.New(codes.Unavailable, err.Error())
		code = chunkv1.ErrorCode_ERROR_CODE_CLOCK_NOT_MONOTONIC
	case errors.Is(err, domain.ErrVolumeDegraded):
		st = status.New(codes.Unavailable, err.Error())
		code = chunkv1.ErrorCode_ERROR_CODE_VOLUME_DEGRADED
	case errors.Is(err, domain.ErrVolumeFailed):
		st = status.New(codes.Unavailable, err.Error())
		code = chunkv1.ErrorCode_ERROR_CODE_VOLUME_FAILED
	default:
		st = status.New(codes.Internal, err.Error())
		code = chunkv1.ErrorCode_ERROR_CODE_INTERNAL
	}

	details := &chunkv1.ErrorDetails{
		Code: code,
	}

	var appContextErr *applicationerrors.ContextError
	if errors.As(err, &appContextErr) {
		details.Chunk = ChunkFromDomain(appContextErr.Chunk())
		details.Volume = VolumeFromDomain(appContextErr.VolumeStatus())

		if chunk := appContextErr.RemotePeerChunk(); chunk != nil {
			details.RemotePeerChunk = RemotePeerChunkFromDomain(chunk)
		}

		if volume := appContextErr.RemotePeerVolumeStatus(); volume != nil {
			details.RemotePeerVolume = VolumeFromDomain(volume)
		}
	}

	st, _ = st.WithDetails(details)

	return st
}
