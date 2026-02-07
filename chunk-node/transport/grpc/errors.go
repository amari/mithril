package grpc

import (
	"errors"

	"github.com/amari/mithril/chunk-node/domain"
	chunkstoreerrors "github.com/amari/mithril/chunk-node/errors"
	chunkv1 "github.com/amari/mithril/gen/go/proto/mithril/chunk/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// mapErrorToStatus converts domain errors to gRPC status with ErrorDetails.
func mapErrorToStatus(err error, volumeState domain.VolumeState) error {
	if err == nil {
		return nil
	}

	// Determine gRPC code and ErrorCode
	var grpcCode codes.Code
	var errorCode chunkv1.ErrorCode

	switch {
	case errors.Is(err, chunkstoreerrors.ErrChunkNotFound):
		grpcCode = codes.NotFound
		errorCode = chunkv1.ErrorCode_ERROR_CODE_NOT_FOUND

	case errors.Is(err, chunkstoreerrors.ErrVersionMismatch):
		grpcCode = codes.FailedPrecondition
		errorCode = chunkv1.ErrorCode_ERROR_CODE_VERSION_MISMATCH

	case errors.Is(err, chunkstoreerrors.ErrWriterKeyConflict):
		grpcCode = codes.AlreadyExists
		errorCode = chunkv1.ErrorCode_ERROR_CODE_ALREADY_EXISTS

	case errors.Is(err, chunkstoreerrors.ErrWriterKeyMismatch):
		grpcCode = codes.PermissionDenied
		errorCode = chunkv1.ErrorCode_ERROR_CODE_WRITER_KEY_MISMATCH

	case errors.Is(err, chunkstoreerrors.ErrChunkWrongState):
		grpcCode = codes.FailedPrecondition
		errorCode = chunkv1.ErrorCode_ERROR_CODE_WRONG_STATE

	case errors.Is(err, chunkstoreerrors.ErrWrongNode):
		grpcCode = codes.FailedPrecondition
		errorCode = chunkv1.ErrorCode_ERROR_CODE_WRONG_NODE

	case errors.Is(err, chunkstoreerrors.ErrVolumeNoSpace):
		grpcCode = codes.ResourceExhausted
		errorCode = chunkv1.ErrorCode_ERROR_CODE_NO_SPACE

	case errors.Is(err, chunkstoreerrors.ErrVolumeDegraded):
		volumeState = domain.VolumeStateDegraded
		grpcCode = codes.Unavailable
		errorCode = chunkv1.ErrorCode_ERROR_CODE_VOLUME_DEGRADED

	case errors.Is(err, chunkstoreerrors.ErrVolumeFailed):
		volumeState = domain.VolumeStateFailed
		grpcCode = codes.Unavailable
		errorCode = chunkv1.ErrorCode_ERROR_CODE_VOLUME_FAILED

	case errors.Is(err, chunkstoreerrors.ErrOffsetOutOfBounds),
		errors.Is(err, chunkstoreerrors.ErrChunkTooLarge),
		errors.Is(err, chunkstoreerrors.ErrWriteSizeMismatch):
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
	var chunkError *chunkstoreerrors.ChunkError
	if errors.As(err, &chunkError) && chunkError.Chunk.ID != nil {
		details.Chunk = &chunkv1.Chunk{
			Id:      chunkError.Chunk.ID[:],
			Version: chunkError.Chunk.Version,
			Size:    chunkError.Chunk.Size,
		}
	}

	// Include local volume state
	details.Volume = &chunkv1.Volume{
		State: mapVolumeState(volumeState),
	}

	// Include remote error details if this wraps a RemoteError
	var remoteError *chunkstoreerrors.RemoteError
	if errors.As(err, &remoteError) {
		if remoteError.Chunk != nil {
			details.RemoteChunk = &chunkv1.Chunk{
				Id:      remoteError.Chunk.ID,
				Version: remoteError.Chunk.Version,
				Size:    remoteError.Chunk.Size,
			}
		}
		if remoteError.Volume != nil {
			details.RemoteVolume = &chunkv1.Volume{
				State: mapVolumeStateFromError(remoteError.Volume.State),
			}
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

// mapVolumeStateFromError converts error volume state to proto volume state.
func mapVolumeStateFromError(state chunkstoreerrors.VolumeState) chunkv1.Volume_State {
	switch state {
	case chunkstoreerrors.VolumeStateOk:
		return chunkv1.Volume_STATE_OK
	case chunkstoreerrors.VolumeStateDegraded:
		return chunkv1.Volume_STATE_DEGRADED
	case chunkstoreerrors.VolumeStateFailed:
		return chunkv1.Volume_STATE_FAILED
	default:
		return chunkv1.Volume_STATE_UNSPECIFIED
	}
}
