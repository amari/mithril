package volume

import (
	"github.com/amari/mithril/chunk-node/domain"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
	"github.com/amari/mithril/chunk-node/volumeerrors"
)

type VolumeAdmissionController struct {
	healthProvider portvolume.VolumeHealthProvider
}

var _ portvolume.VolumeAdmissionController = (*VolumeAdmissionController)(nil)

func NewVolumeAdmissionController(healthProvider portvolume.VolumeHealthProvider) portvolume.VolumeAdmissionController {
	return &VolumeAdmissionController{
		healthProvider: healthProvider,
	}
}

func (c *VolumeAdmissionController) AdmitRead(id domain.VolumeID) error {
	health := c.healthProvider.GetVolumeHealth(id)

	if health == nil {
		// FIXME: maybe we need to return an error here instead of allowing the read to proceed?
		return nil
	}

	switch health.State {
	case domain.VolumeStateFailed:
		return volumeerrors.WithState(
			volumeerrors.ErrFailed, volumeerrors.StateFailed,
		)
	}

	return nil
}

func (c *VolumeAdmissionController) AdmitWrite(id domain.VolumeID) error {
	health := c.healthProvider.GetVolumeHealth(id)

	if health == nil {
		// FIXME: maybe we need to return an error here instead of allowing the write to proceed?
		return nil
	}

	switch health.State {
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

func (c *VolumeAdmissionController) AdmitStat(id domain.VolumeID) error {
	// Always allow stat operations, even on failed volumes.
	return nil
}
