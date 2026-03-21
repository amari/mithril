package applicationcommands

import (
	applicationerrors "github.com/amari/mithril/mithril-node-go/internal/application/errors"
	"github.com/amari/mithril/mithril-node-go/internal/domain"
)

func WithChunk(err error, c *domain.ReadyChunk) error {
	return applicationerrors.ContextErrorWithChunk(err, c)
}

func WithVolumeStatus(err error, s domain.VolumeStatus) error {
	return applicationerrors.ContextErrorWithVolumeStatus(err, s)
}

func WithChunkAndVolumeStatus(err error, c *domain.ReadyChunk, s domain.VolumeStatus) error {
	return applicationerrors.ContextErrorWithChunkAndVolumeStatus(err, c, s)
}
