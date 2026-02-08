package grpc

import (
	"github.com/amari/mithril/chunk-node/chunkerrors"
	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/nodeerrors"
	"github.com/amari/mithril/chunk-node/volumeerrors"
	chunkv1 "github.com/amari/mithril/gen/go/proto/mithril/chunk/v1"
	"google.golang.org/grpc/status"
)

type remoteError struct {
	Msg       string
	ErrorCode chunkv1.ErrorCode
}

func (e *remoteError) Error() string {
	return e.Msg
}

// extractErrorDetails converts a gRPC error with ErrorDetails into a RemoteError.
// Returns nil if the error doesn't contain ErrorDetails or isn't a gRPC error.
func extractErrorDetails(err error) error {
	st, ok := status.FromError(err)
	if !ok {
		return nil
	}

	for _, detail := range st.Details() {
		if errDetails, ok := detail.(*chunkv1.ErrorDetails); ok {
			var err error = &remoteError{
				Msg:       st.Message(),
				ErrorCode: errDetails.Code,
			}

			// Extract remote node ID (from remote fields in remote error)
			if errDetails.RemoteNode != nil {
				err = nodeerrors.WithRemoteNode(err, domain.NodeID(errDetails.RemoteNode.Id))
			}

			// Extract remote volume state (from remote fields in remote error)
			if errDetails.Volume != nil {
				err = volumeerrors.WithRemoteState(err, mapProtoVolumeState(errDetails.RemoteVolume.State))
			}

			// Extract remote chunk state (from remote fields in remote error)
			if errDetails.Chunk != nil {
				err = chunkerrors.WithRemoteChunk(err, domain.ChunkID(errDetails.RemoteChunk.Id), errDetails.RemoteChunk.Version, errDetails.RemoteChunk.Size)
			}

			// Extract volume state (from local fields in remote error)
			if errDetails.Volume != nil {
				err = volumeerrors.WithState(err, mapProtoVolumeState(errDetails.Volume.State))
			}

			// Extract chunk state (from local fields in remote error)
			if errDetails.Chunk != nil {
				// FIXME: validate that chunk ID is a valid domain.ChunkID before converting
				err = chunkerrors.WithChunk(err, domain.ChunkID(errDetails.Chunk.Id), errDetails.Chunk.Version, errDetails.Chunk.Size)
			}

			return err
		}
	}

	return nil
}

// mapProtoVolumeState converts proto volume state to error volume state.
func mapProtoVolumeState(state chunkv1.Volume_State) volumeerrors.State {
	switch state {
	case chunkv1.Volume_STATE_OK:
		return volumeerrors.StateOK
	case chunkv1.Volume_STATE_DEGRADED:
		return volumeerrors.StateDegraded
	case chunkv1.Volume_STATE_FAILED:
		return volumeerrors.StateFailed
	default:
		return volumeerrors.StateUnknown
	}
}
