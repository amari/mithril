package admission

import (
	"context"

	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/volumeerrors"
)

func AdmitReadWithVolumeHealth(ctx context.Context, volumeHealth *domain.VolumeHealth) error {
	if volumeHealth == nil {
		return nil
	}

	switch volumeHealth.State {
	case domain.VolumeStateFailed:
		return volumeerrors.WithState(
			volumeerrors.ErrFailed, volumeerrors.StateFailed,
		)
	}

	return nil
}

func AdmitWriteWithVolumeHealth(ctx context.Context, volumeHealth *domain.VolumeHealth) error {
	if volumeHealth == nil {
		return nil
	}

	switch volumeHealth.State {
	case domain.VolumeStateDegraded:
		return volumeerrors.WithState(
			volumeerrors.ErrDegraded, volumeerrors.StateDegraded,
		)
	case domain.VolumeStateFailed:
		return volumeerrors.WithState(
			volumeerrors.ErrFailed, volumeerrors.StateFailed,
		)
	}

	return nil
}
