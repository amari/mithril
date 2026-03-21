package adaptersremotepeer

import (
	chunkv1 "github.com/amari/mithril/gen/go/proto/mithril/chunk/v1"
	applicationerrors "github.com/amari/mithril/mithril-node-go/internal/application/errors"
	"github.com/amari/mithril/mithril-node-go/internal/domain"
	"google.golang.org/grpc/status"
)

func StatusToError(st *status.Status) error {
	if st == nil {
		return nil
	}

	for _, detail := range st.Details() {
		d, ok := detail.(*chunkv1.ErrorDetails)
		if !ok {
			continue
		}

		var err error = domain.ErrChunkInternal

		switch d.Code {
		case chunkv1.ErrorCode_ERROR_CODE_UNSPECIFIED:
		case chunkv1.ErrorCode_ERROR_CODE_INTERNAL:
			err = domain.ErrChunkInternal
		case chunkv1.ErrorCode_ERROR_CODE_CHUNK_NOT_FOUND:
			err = domain.ErrChunkNotFound
		case chunkv1.ErrorCode_ERROR_CODE_CHUNK_ALREADY_EXISTS:
			err = domain.ErrChunkAlreadyExists
		case chunkv1.ErrorCode_ERROR_CODE_CHUNK_CORRUPTED:
			err = domain.ErrChunkCorrupted
		case chunkv1.ErrorCode_ERROR_CODE_CHUNK_WRITE_INCOMPLETE:
			err = domain.ErrChunkWriteIncomplete
		case chunkv1.ErrorCode_ERROR_CODE_CHUNK_INVALID_OPERATION:
			err = domain.ErrChunkInvalidOperation
		case chunkv1.ErrorCode_ERROR_CODE_CHUNK_INVALID_ID:
			err = domain.ErrChunkInvalidID
		case chunkv1.ErrorCode_ERROR_CODE_CHUNK_INVALID_VERSION:
			err = domain.ErrChunkInvalidVersion
		case chunkv1.ErrorCode_ERROR_CODE_CHUNK_INVALID_RANGE:
			err = domain.ErrChunkInvalidRange
		case chunkv1.ErrorCode_ERROR_CODE_CLOCK_NOT_MONOTONIC:
			err = domain.ErrClockNotMonotonic
		case chunkv1.ErrorCode_ERROR_CODE_VOLUME_DEGRADED:
			err = domain.ErrVolumeDegraded
		case chunkv1.ErrorCode_ERROR_CODE_VOLUME_FAILED:
			err = domain.ErrVolumeFailed
		default:
			err = domain.ErrChunkInternal
		}

		contextErr := applicationerrors.NewContextError(err)

		if d.Chunk != nil {
			chunk, err := ChunkFromProto(d.Chunk)
			if err == nil {
				contextErr = contextErr.WithRemotePeerChunk(chunk)
			}
		}

		if d.Volume != nil {
			contextErr = contextErr.WithRemotePeerVolumeStatus(VolumeStatusFromProto(d.Volume))
		}

		return contextErr
	}

	return domain.ErrChunkInternal
}
