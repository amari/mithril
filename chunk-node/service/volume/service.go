package volume

import (
	"context"
	"errors"
	"time"

	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/port"
	"github.com/amari/mithril/chunk-node/port/volume"
	"github.com/amari/mithril/chunk-node/volumeerrors"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
)

type VolumeService struct {
	nodeIdentityRepo  port.NodeIdentityRepository
	idAlloc           volume.VolumeIDAllocator
	directoryExpert   volume.DirectoryVolumeExpert
	manager           *VolumeManager
	picker            volume.VolumePicker
	attributeRegistry volume.VolumeAttributeRegistry

	// Health and stats management
	healthTracker *VolumeHealthTracker
	statsManager  *VolumeStatsManager
	log           *zerolog.Logger
}

func NewVolumeService(
	nodeIdentityRepo port.NodeIdentityRepository,
	idAlloc volume.VolumeIDAllocator,
	directoryExpert volume.DirectoryVolumeExpert,
	manager *VolumeManager,
	picker volume.VolumePicker,
	attributeRegistry volume.VolumeAttributeRegistry,
	healthTracker *VolumeHealthTracker,
	statsManager *VolumeStatsManager,
	log *zerolog.Logger,
) *VolumeService {
	return &VolumeService{
		nodeIdentityRepo:  nodeIdentityRepo,
		idAlloc:           idAlloc,
		directoryExpert:   directoryExpert,
		manager:           manager,
		picker:            picker,
		attributeRegistry: attributeRegistry,
		healthTracker:     healthTracker,
		statsManager:      statsManager,
		log:               log,
	}
}

func (s *VolumeService) AddDirectoryVolume(ctx context.Context, path string, formatIfNeeded bool) error {
	nodeIdentity, err := s.nodeIdentityRepo.LoadNodeIdentity(ctx)
	if err != nil {
		return err
	}

	_, _, err = s.directoryExpert.ReadDirectoryVolume(ctx, path)
	if err != nil {
		if errors.Is(err, volumeerrors.ErrNotFormatted) && formatIfNeeded {
			volumeID, err := s.idAlloc.AllocateVolumeID(ctx)
			if err != nil {
				return err
			}

			err = s.directoryExpert.FormatDirectoryVolume(ctx, path, nodeIdentity.NodeID, volumeID)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}

	vol, err := s.directoryExpert.OpenDirectoryVolume(ctx, path, nodeIdentity.NodeID)
	if err != nil {
		return err
	}

	volumeID := vol.ID()

	// Start stats monitoring - must succeed
	pollInterval := 10 * time.Second // TODO: make configurable
	if err := s.statsManager.AddDirectoryVolume(volumeID, path, pollInterval); err != nil {
		// Failed to setup stats, close volume and fail
		vol.Close()
		return err
	}

	// Start health tracking - must succeed
	healthConfig := defaultHealthConfig() // TODO: make configurable per volume
	if err := s.healthTracker.AddVolume(volumeID, healthConfig); err != nil {
		// Failed to setup health, clean up stats and volume, then fail
		s.statsManager.RemoveVolume(volumeID)
		vol.Close()
		return err
	}

	// All setup successful, add to manager and picker
	s.manager.AddVolume(vol)
	s.picker.SetVolumeIDs(s.manager.VolumeIDs())

	// Register static attributes that won't change
	s.attributeRegistry.AddAttributes(volumeID,
		attribute.String("volume.path", path),
		attribute.String("volume.type", "directory"),
	)

	s.log.Info().
		Uint16("volume_id", uint16(volumeID)).
		Str("path", path).
		Msg("successfully added directory volume")

	return nil
}

func (s *VolumeService) CloseAllVolumes(ctx context.Context) error {
	var errs []error

	s.picker.SetVolumeIDs(nil)

	// Stop health tracking for all volumes
	s.healthTracker.ClearVolumes()

	vols := s.manager.Volumes()

	for _, vol := range vols {
		id := vol.ID()

		// Clean up attributes
		s.attributeRegistry.RemoveAllAttributes(id)

		// Stop stats monitoring
		if err := s.statsManager.RemoveVolume(id); err != nil {
			errs = append(errs, err)
		}

		err := vol.Close()
		if err != nil {
			errs = append(errs, err)
		}

		s.manager.RemoveVolume(id)
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}

// RecordOperationError is called by handlers after volume operations fail.
// Updates health state based on error classification.
func (s *VolumeService) RecordOperationError(ctx context.Context, volumeID domain.VolumeID, err error) {
	s.healthTracker.RecordError(ctx, volumeID, err)
}

// RecordOperationSuccess is called by handlers after successful volume operations.
// May contribute to recovery from degraded state.
func (s *VolumeService) RecordOperationSuccess(ctx context.Context, volumeID domain.VolumeID) {
	s.healthTracker.RecordSuccess(ctx, volumeID)
}
