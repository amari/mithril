package applicationcommands

import (
	applicationerrors "github.com/amari/mithril/mithril-node-go/internal/application/errors"
	"github.com/amari/mithril/mithril-node-go/internal/domain"
)

func WithChunk(err error, c *domain.ReadyChunk) error {
	return applicationerrors.WithChunk(err, c)
}

func WithVolumeStatus(err error, s domain.VolumeStatus) error {
	return applicationerrors.WithVolumeStatus(err, s)
}

func WithChunkAndVolumeStatus(err error, c *domain.ReadyChunk, s domain.VolumeStatus) error {
	return applicationerrors.WithChunkAndVolumeStatus(err, c, s)
}
