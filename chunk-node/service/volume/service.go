package volume

import (
	"context"
	"errors"
	"maps"
	"strings"
	"time"

	adaptervolumecharacteristics "github.com/amari/mithril/chunk-node/adapter/volume/characteristics"
	"github.com/amari/mithril/chunk-node/adapter/volume/directory"
	volumedirectorystatscollector "github.com/amari/mithril/chunk-node/adapter/volume/directory/statscollector"
	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/port"

	portvolume "github.com/amari/mithril/chunk-node/port/volume"
	"github.com/amari/mithril/chunk-node/volumeerrors"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
)

type VolumeService struct {
	nodeIdentityRepo port.NodeIdentityRepository
	idAlloc          portvolume.VolumeIDAllocator
	directoryExpert  portvolume.DirectoryVolumeExpert
	manager          *VolumeManager
	picker           portvolume.VolumePicker

	log *zerolog.Logger

	characteristicsProviders map[domain.VolumeID]portvolume.VolumeCharacteristicsProvider
	attributes               map[domain.VolumeID][]attribute.KeyValue
	labels                   map[domain.VolumeID]map[string]string
	labelProviders           map[domain.VolumeID][]portvolume.VolumeLabelProvider
	labelIndex               map[string]map[string][]domain.VolumeID
	labelIDSets              map[string]map[string]*domain.VolumeIDSet
	loggerFields             map[domain.VolumeID][]any
	stats                    map[domain.VolumeID]portvolume.VolumeStatsProvider

	labelIndexPublisher portvolume.VolumeIDSetLabelIndexesPublisher
}

var (
	_ portvolume.VolumeIDToCharacteristicsIndex = (*VolumeService)(nil)
	_ portvolume.VolumeLabelToIDsIndex          = (*VolumeService)(nil)
	_ portvolume.VolumeLabelToIDSetIndex        = (*VolumeService)(nil)
	_ portvolume.VolumeTelemetryProvider        = (*VolumeService)(nil)
)

func NewVolumeService(
	nodeIdentityRepo port.NodeIdentityRepository,
	idAlloc portvolume.VolumeIDAllocator,
	directoryExpert portvolume.DirectoryVolumeExpert,
	manager *VolumeManager,
	picker portvolume.VolumePicker,
	log *zerolog.Logger,
	labelIndexPublisher portvolume.VolumeIDSetLabelIndexesPublisher,
) *VolumeService {
	return &VolumeService{
		nodeIdentityRepo: nodeIdentityRepo,
		idAlloc:          idAlloc,
		directoryExpert:  directoryExpert,
		manager:          manager,
		picker:           picker,
		log:              log,

		characteristicsProviders: make(map[domain.VolumeID]portvolume.VolumeCharacteristicsProvider),
		attributes:               make(map[domain.VolumeID][]attribute.KeyValue),
		labels:                   make(map[domain.VolumeID]map[string]string),
		labelProviders:           make(map[domain.VolumeID][]portvolume.VolumeLabelProvider),
		labelIndex:               make(map[string]map[string][]domain.VolumeID),
		labelIDSets:              make(map[string]map[string]*domain.VolumeIDSet),
		loggerFields:             make(map[domain.VolumeID][]any),
		stats:                    make(map[domain.VolumeID]portvolume.VolumeStatsProvider),

		labelIndexPublisher: labelIndexPublisher,
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

	// Create and store volume characteristics provider
	characteristicsProvider, err := directory.NewCharacteristicsProvider(path)
	if err != nil {
		vol.Close()

		return err
	}
	s.characteristicsProviders[volumeID] = characteristicsProvider

	// Collect attributes and logger fields
	attributes := buildVolumeAttributes(characteristicsProvider.GetVolumeCharacteristics())
	attributes = append(attributes,
		attribute.Int64("volume.id", int64(volumeID)),
		attribute.String("volume.path", path),
	)
	s.attributes[volumeID] = attributes
	s.loggerFields[volumeID] = buildVolumeLoggerFieldsWithAttributes(attributes)

	// Collect labels and update indexes
	labelProviders := []portvolume.VolumeLabelProvider{
		adaptervolumecharacteristics.NewLabelProvider(characteristicsProvider.GetVolumeCharacteristics()),
	}
	s.labelProviders[volumeID] = labelProviders

	labels := map[string]string{}
	for _, provider := range labelProviders {
		// TODO: watch every label provider

		maps.Copy(labels, provider.GetVolumeLabels())
	}
	s.labels[volumeID] = labels

	for key, value := range labels {
		volumeIDByLabelValue, ok := s.labelIndex[key]
		if !ok {
			volumeIDByLabelValue = make(map[string][]domain.VolumeID)
			s.labelIndex[key] = volumeIDByLabelValue
		}
		volumeIDByLabelValue[value] = append(volumeIDByLabelValue[value], volumeID)

		// Update bitset index
		volumeIDSetByLabelValue, ok := s.labelIDSets[key]
		if !ok {
			volumeIDSetByLabelValue = make(map[string]*domain.VolumeIDSet)
			s.labelIDSets[key] = volumeIDSetByLabelValue
		}
		bitset, ok := volumeIDSetByLabelValue[value]
		if !ok {
			bitset = domain.NewVolumeIDSet()

			volumeIDSetByLabelValue[value] = bitset
		}
		bitset.Add(volumeID)
	}

	// Publish updated label indexes
	if s.labelIndexPublisher != nil {
		s.labelIndexPublisher.PublishVolumeIDSetLabelIndexes(s.labelIDSets)
	}

	// Start stats collecting
	if statsCollector, err := volumedirectorystatscollector.NewDirectoryVolumeStatsProvider(volumedirectorystatscollector.DirectoryVolumeStatsProviderOptions{
		VolumeID:     volumeID,
		Path:         path,
		PollInterval: 1 * time.Second, // TODO: make configurable
	}); err != nil {
		vol.Close()
		return err
	} else {
		s.stats[volumeID] = statsCollector
	}

	// Start health tracking - must succeed
	// TODO

	// All setup successful, add to manager and picker
	s.manager.AddVolume(vol)
	s.picker.SetVolumeIDs(s.manager.VolumeIDs())

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
	// TODO

	vols := s.manager.Volumes()

	for _, vol := range vols {
		id := vol.ID()

		// Stop collecting stats
		if collector, ok := s.stats[id]; ok {
			if stopper, ok := collector.(interface{ Stop() error }); ok {
				if err := stopper.Stop(); err != nil {
					errs = append(errs, err)
				}
			}

			delete(s.stats, id)
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
	//s.healthTracker.RecordError(ctx, volumeID, err)
}

// RecordOperationSuccess is called by handlers after successful volume operations.
// May contribute to recovery from degraded state.
func (s *VolumeService) RecordOperationSuccess(ctx context.Context, volumeID domain.VolumeID) {
	//s.healthTracker.RecordSuccess(ctx, volumeID)
}

// GetVolumeCharacteristicsByID implements VolumeIDToCharacteristicsIndex.
func (s *VolumeService) GetVolumeCharacteristicsByID(id domain.VolumeID) *domain.VolumeCharacteristics {
	characteristics, ok := s.characteristicsProviders[id]
	if !ok {
		return nil
	}

	return characteristics.GetVolumeCharacteristics()
}

// GetVolumeAttributes implements VolumeTelemetryProvider.
func (s *VolumeService) GetVolumeAttributes(id domain.VolumeID) []attribute.KeyValue {
	attributes, ok := s.attributes[id]
	if !ok {
		return nil
	}

	return attributes
}

// GetVolumeLoggerFields implements VolumeTelemetryProvider.
func (s *VolumeService) GetVolumeLoggerFields(id domain.VolumeID) []any {
	fields, ok := s.loggerFields[id]
	if !ok {
		return nil
	}

	return fields
}

// GetVolumeLabels implements VolumeLabelProvider.
func (s *VolumeService) GetVolumeLabels(id domain.VolumeID) map[string]string {
	labels, ok := s.labels[id]
	if !ok {
		return nil
	}

	return labels
}

func (s *VolumeService) GetVolumeIDsByLabel(key, value string) []domain.VolumeID {
	a, ok := s.labelIndex[key]
	if !ok {
		return nil
	}
	b, ok := a[value]
	if !ok {
		return nil
	}
	return b
}

func (s *VolumeService) GetVolumeIDSetByLabel(key, value string) *domain.VolumeIDSet {
	a, ok := s.labelIDSets[key]
	if !ok {
		return nil
	}
	b, ok := a[value]
	if !ok {
		return nil
	}
	return b
}

func (s *VolumeService) GetAllVolumeIDSets() map[string]map[string]*domain.VolumeIDSet {
	return s.labelIDSets
}

// GetVolumeStats implements VolumeIDToStatsIndex.
func (s *VolumeService) GetVolumeStatsByID(id domain.VolumeID) *domain.VolumeStats {
	statsCollector, ok := s.stats[id]

	if !ok {
		return statsCollector.GetVolumeStats()
	}

	return nil
}

func (s *VolumeService) GetVolumeHealth(id domain.VolumeID) *domain.VolumeHealth {
	return &domain.VolumeHealth{
		State: domain.VolumeStateOK,
	}
}

func (s *VolumeService) CheckVolumeHealth(id domain.VolumeID) *domain.VolumeHealth {
	return s.GetVolumeHealth(id)
}

func buildVolumeAttributes(characteristics *domain.VolumeCharacteristics) []attribute.KeyValue {
	if characteristics == nil {
		return nil
	}

	attributes := []attribute.KeyValue{
		attribute.String("device.name", characteristics.DeviceName),
	}

	if characteristics.SerialNumber != "" {
		attributes = append(attributes, attribute.String("device.id", characteristics.SerialNumber))
	}

	if characteristics.Vendor != "" {
		attributes = append(attributes, attribute.String("device.vendor", characteristics.Vendor))
	}

	if characteristics.Model != "" {
		attributes = append(attributes, attribute.String("device.model", characteristics.Model))
	}

	if characteristics.Medium != "" {
		var medium string
		switch characteristics.Medium {
		case domain.MediumTypeRotational:
			medium = "hdd"
		case domain.MediumTypeSolidState:
			medium = "ssd"
		default:
			medium = ""
		}

		if medium != "" {
			attributes = append(attributes, attribute.String("device.medium", medium))
		}
	}
	if characteristics.Interconnect != "" {
		attributes = append(attributes, attribute.String("device.interconnect", string(characteristics.Interconnect)))
	}
	if characteristics.Protocol != "" {
		attributes = append(attributes, attribute.String("device.protocol", string(characteristics.Protocol)))
	}

	if characteristics.FileSystem != "" {
		attributes = append(attributes, attribute.String("device.filesystem", strings.ToLower(characteristics.FileSystem.String())))
	}

	return attributes
}

func buildVolumeLoggerFieldsWithAttributes(attributes []attribute.KeyValue) []any {
	if len(attributes) == 0 {
		return nil
	}

	fields := make([]any, 0, len(attributes)*2)

	for _, attr := range attributes {
		fields = append(fields, attr.Key, attr.Value.AsInterface())
	}

	return fields
}
