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

	if errors.Is(err, domain.ErrChunkInternal) {
		st = status.New(codes.Internal, err.Error())
		code = chunkv1.ErrorCode_ERROR_CODE_INTERNAL
	}

	if errors.Is(err, domain.ErrChunkNotFound) {
		st = status.New(codes.NotFound, err.Error())
		code = chunkv1.ErrorCode_ERROR_CODE_NOT_FOUND
	}

	if errors.Is(err, domain.ErrChunkAlreadyExists) {
		st = status.New(codes.AlreadyExists, err.Error())
		code = chunkv1.ErrorCode_ERROR_CODE_ALREADY_EXISTS
	}

	if errors.Is(err, domain.ErrChunkWrongState) {
		st = status.New(codes.FailedPrecondition, err.Error())
		code = chunkv1.ErrorCode_ERROR_CODE_WRONG_STATE
	}

	if errors.Is(err, domain.ErrChunkVersionMismatch) {
		st = status.New(codes.FailedPrecondition, err.Error())
		code = chunkv1.ErrorCode_ERROR_CODE_VERSION_MISMATCH
	}

	if errors.Is(err, domain.ErrChunkInvalidArgument) {
		st = status.New(codes.InvalidArgument, err.Error())
		code = chunkv1.ErrorCode_ERROR_CODE_INVALID_ARGUMENT
	}

	if errors.Is(err, domain.ErrVolumeNoSpace) {
		st = status.New(codes.ResourceExhausted, err.Error())
		code = chunkv1.ErrorCode_ERROR_CODE_NO_SPACE
	}

	if errors.Is(err, domain.ErrVolumeDegraded) {
		st = status.New(codes.Unavailable, err.Error())
		code = chunkv1.ErrorCode_ERROR_CODE_VOLUME_DEGRADED
	}

	if errors.Is(err, domain.ErrVolumeFailed) {
		st = status.New(codes.Unavailable, err.Error())
		code = chunkv1.ErrorCode_ERROR_CODE_VOLUME_FAILED
	}

	if errors.Is(err, domain.ErrChunkCorrupted) {
		st = status.New(codes.DataLoss, err.Error())
		code = chunkv1.ErrorCode_ERROR_CODE_DATA_LOSS
	}

	if errors.Is(err, domain.ErrChunkShortWrite) {
		st = status.New(codes.Unavailable, err.Error())
		code = chunkv1.ErrorCode_ERROR_CODE_SHORT_WRITE
	}

	if errors.Is(err, domain.ErrChunkIDCollision) {
		st = status.New(codes.Unavailable, err.Error())
		code = chunkv1.ErrorCode_ERROR_CODE_CHUNK_ID_COLLISION
	}

	if errors.Is(err, domain.ErrClockRegressionDetected) {
		st = status.New(codes.Unavailable, err.Error())
		code = chunkv1.ErrorCode_ERROR_CODE_CLOCK_REGRESSION_DETECTED
	}

	details := &chunkv1.ErrorDetails{
		Code: code,
	}

	var appContextErr *applicationerrors.ContextError
	if errors.As(err, &appContextErr) {
		details.Chunk = ChunkFromDomain(appContextErr.Chunk())
		details.Volume = VolumeFromDomain(appContextErr.VolumeStatus())
	}

	st, _ = st.WithDetails(details)

	return st
}
