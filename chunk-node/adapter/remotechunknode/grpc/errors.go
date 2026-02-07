package grpc

import (
	chunkstoreerrors "github.com/amari/mithril/chunk-node/errors"
	chunkv1 "github.com/amari/mithril/gen/go/proto/mithril/chunk/v1"
	"google.golang.org/grpc/status"
)

// extractRemoteError converts a gRPC error with ErrorDetails into a RemoteError.
// Returns nil if the error doesn't contain ErrorDetails or isn't a gRPC error.
func extractRemoteError(err error) *chunkstoreerrors.RemoteError {
	st, ok := status.FromError(err)
	if !ok {
		return nil
	}

	for _, detail := range st.Details() {
		if errDetails, ok := detail.(*chunkv1.ErrorDetails); ok {
			remoteErr := &chunkstoreerrors.RemoteError{
				Msg:       st.Message(),
				ErrorCode: errDetails.Code,
			}

			// Extract chunk state (from local fields in remote error)
			if errDetails.Chunk != nil {
				remoteErr.Chunk = &chunkstoreerrors.Chunk{
					ID:      errDetails.Chunk.Id,
					Version: errDetails.Chunk.Version,
					Size:    errDetails.Chunk.Size,
				}
			}

			// Extract volume state (from local fields in remote error)
			if errDetails.Volume != nil {
				remoteErr.Volume = &chunkstoreerrors.Volume{
					State: mapProtoVolumeState(errDetails.Volume.State),
				}
			}

			return remoteErr
		}
	}

	return nil
}

// mapProtoVolumeState converts proto volume state to error volume state.
func mapProtoVolumeState(state chunkv1.Volume_State) chunkstoreerrors.VolumeState {
	switch state {
	case chunkv1.Volume_STATE_OK:
		return chunkstoreerrors.VolumeStateOk
	case chunkv1.Volume_STATE_DEGRADED:
		return chunkstoreerrors.VolumeStateDegraded
	case chunkv1.Volume_STATE_FAILED:
		return chunkstoreerrors.VolumeStateFailed
	default:
		return chunkstoreerrors.VolumeStateUnknown
	}
}
