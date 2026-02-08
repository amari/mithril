package chunk

import (
	"github.com/amari/mithril/chunk-node/chunkerrors"
	"github.com/amari/mithril/chunk-node/domain"
)

func ChunkIDFromBytes(data []byte) (domain.ChunkID, error) {
	var id domain.ChunkID

	if len(data) != len(id) {
		return id, chunkerrors.ErrInvalidID
	}

	copy(id[:], data)

	return id, nil
}
