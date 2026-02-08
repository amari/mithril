package grpc

import (
	"errors"

	"github.com/amari/mithril/chunk-node/chunkerrors"
	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/nodeerrors"
	"github.com/amari/mithril/chunk-node/volumeerrors"
	chunkv1 "github.com/amari/mithril/gen/go/proto/mithril/chunk/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// mapErrorToStatus converts domain errors to gRPC status with ErrorDetails.
func mapErrorToStatus(err error) error {
	if err == nil {
		return nil
	}

	// Determine gRPC code and ErrorCode and VolumeState based on error type
	var grpcCode codes.Code
	var errorCode chunkv1.ErrorCode
	volumeState := domain.VolumeStateUnknown

	switch {
	case errors.Is(err, chunkerrors.ErrNotFound):
		grpcCode = codes.NotFound
		errorCode = chunkv1.ErrorCode_ERROR_CODE_NOT_FOUND

	case errors.Is(err, chunkerrors.ErrVersionMismatch):
		grpcCode = codes.FailedPrecondition
		errorCode = chunkv1.ErrorCode_ERROR_CODE_VERSION_MISMATCH

	case errors.Is(err, chunkerrors.ErrWriterKeyConflict):
		grpcCode = codes.AlreadyExists
		errorCode = chunkv1.ErrorCode_ERROR_CODE_ALREADY_EXISTS

	case errors.Is(err, chunkerrors.ErrWriterKeyMismatch):
		grpcCode = codes.PermissionDenied
		errorCode = chunkv1.ErrorCode_ERROR_CODE_WRITER_KEY_MISMATCH

	case errors.Is(err, chunkerrors.ErrWrongState):
		grpcCode = codes.FailedPrecondition
		errorCode = chunkv1.ErrorCode_ERROR_CODE_WRONG_STATE

	case errors.Is(err, chunkerrors.ErrWrongNode):
		grpcCode = codes.FailedPrecondition
		errorCode = chunkv1.ErrorCode_ERROR_CODE_WRONG_NODE

	case errors.Is(err, volumeerrors.ErrNoSpace):
		grpcCode = codes.ResourceExhausted
		errorCode = chunkv1.ErrorCode_ERROR_CODE_NO_SPACE

	case errors.Is(err, volumeerrors.ErrDegraded):
		volumeState = domain.VolumeStateDegraded
		grpcCode = codes.Unavailable
		errorCode = chunkv1.ErrorCode_ERROR_CODE_VOLUME_DEGRADED

	case errors.Is(err, volumeerrors.ErrFailed):
		volumeState = domain.VolumeStateFailed
		grpcCode = codes.Unavailable
		errorCode = chunkv1.ErrorCode_ERROR_CODE_VOLUME_FAILED

	case errors.Is(err, chunkerrors.ErrOffsetOutOfBounds),
		errors.Is(err, chunkerrors.ErrTooLarge),
		errors.Is(err, chunkerrors.ErrWriteSizeMismatch):
		grpcCode = codes.InvalidArgument
		errorCode = chunkv1.ErrorCode_ERROR_CODE_INVALID_ARGUMENT

	default:
		grpcCode = codes.Internal
		errorCode = chunkv1.ErrorCode_ERROR_CODE_INTERNAL
	}

	// Build ErrorDetails
	details := &chunkv1.ErrorDetails{
		Code: errorCode,
	}

	// Include local chunk metadata if available
	var chunkError *chunkerrors.ChunkError
	if errors.As(err, &chunkError) {
		details.Chunk = &chunkv1.Chunk{
			Id:      chunkError.ChunkID().Bytes(),
			Version: chunkError.ChunkVersion(),
			Size:    chunkError.ChunkSize(),
		}
	}

	// Include local volume state
	var volumeStateError *volumeerrors.StateError
	if errors.As(err, &volumeStateError) {
		if volumeState == domain.VolumeStateUnknown || volumeState == domain.VolumeStateOK {
			volumeState = volumeStateError.State()
		}

		details.Volume = &chunkv1.Volume{
			State: mapVolumeState(volumeState),
		}
	}

	// Include remote error details if this wraps a RemoteError
	var remoteNodeError *nodeerrors.RemoteNodeError
	if errors.As(err, &remoteNodeError) {
		details.RemoteNode = &chunkv1.Node{
			Id: uint32(remoteNodeError.RemoteNodeID()),
		}
	}
	var remoteVolumeStateError *volumeerrors.RemoteStateError
	if errors.As(err, &remoteVolumeStateError) {
		details.RemoteVolume = &chunkv1.Volume{
			State: mapVolumeState(remoteVolumeStateError.RemoteState()),
		}
	}
	var remoteChunkError *chunkerrors.RemoteChunkError
	if errors.As(err, &remoteChunkError) {
		details.RemoteChunk = &chunkv1.Chunk{
			Id:      remoteChunkError.RemoteChunkID().Bytes(),
			Version: remoteChunkError.RemoteChunkVersion(),
			Size:    remoteChunkError.RemoteChunkSize(),
		}
	}

	// Create status with details
	st := status.New(grpcCode, err.Error())
	st, err = st.WithDetails(details)
	if err != nil {
		// If adding details fails, return without details
		return status.Error(grpcCode, err.Error())
	}

	return st.Err()
}

// mapVolumeState converts domain health status to proto volume state.
func mapVolumeState(health domain.VolumeState) chunkv1.Volume_State {
	switch health {
	case domain.VolumeStateOK:
		return chunkv1.Volume_STATE_OK
	case domain.VolumeStateDegraded:
		return chunkv1.Volume_STATE_DEGRADED
	case domain.VolumeStateFailed:
		return chunkv1.Volume_STATE_FAILED
	default:
		return chunkv1.Volume_STATE_OK
	}
}
