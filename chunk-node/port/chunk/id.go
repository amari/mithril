package chunk

import (
	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/errors"
)

func ChunkIDFromBytes(data []byte) (domain.ChunkID, error) {
	var id domain.ChunkID

	if len(data) != len(id) {
		return id, errors.ErrChunkNotFound
	}

	copy(id[:], data)

	return id, nil
}
