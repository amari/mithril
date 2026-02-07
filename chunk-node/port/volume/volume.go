package volume

import (
	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/port"
)

type Volume interface {
	Close() error

	ID() domain.VolumeID

	Chunks() port.ChunkStore
}
