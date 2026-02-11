package volume

import (
	"context"
	"sync"

	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/port/volume"
	"github.com/amari/mithril/chunk-node/volumeerrors"
	"github.com/rs/zerolog"
)

// VolumeLabelManager implements VolumeLabelProvider and manages per-volume label collection.
// It knows how to create platform-specific label collectors for different volume types
// and manages publishing labels to watchers.
type VolumeLabelManager struct {
	log *zerolog.Logger

	mu         sync.RWMutex
	collectors map[domain.VolumeID]volume.VolumeLabelCollector
	labels     map[domain.VolumeID]map[string]string
	watchers   map[domain.VolumeID]*labelWatcher
}

var _ volume.VolumeLabelProvider = (*VolumeLabelManager)(nil)

type labelWatcher struct {
	volumeID domain.VolumeID

	// Subscriber management
	subMu       sync.RWMutex
	subscribers map[*labelSubscriber]struct{}
}

type labelSubscriber struct {
	ch     chan map[string]string
	ctx    context.Context
	cancel context.CancelFunc
}

func NewVolumeLabelManager(log *zerolog.Logger) *VolumeLabelManager {
	return &VolumeLabelManager{
		log:        log,
		collectors: make(map[domain.VolumeID]volume.VolumeLabelCollector),
		labels:     make(map[domain.VolumeID]map[string]string),
		watchers:   make(map[domain.VolumeID]*labelWatcher),
	}
}

// VolumeLabels implements VolumeLabelProvider by returning cached labels for the volume.
func (m *VolumeLabelManager) VolumeLabels(ctx context.Context, volumeID domain.VolumeID) (map[string]string, error) {
	m.mu.RLock()
	labels, exists := m.labels[volumeID]
	m.mu.RUnlock()

	if !exists {
		return nil, volumeerrors.ErrNotFound
	}

	// Return a copy to prevent mutation
	result := make(map[string]string, len(labels))
	for k, v := range labels {
		result[k] = v
	}

	return result, nil
}

// AddDirectoryVolume creates a label collector for a directory volume and collects labels.
func (m *VolumeLabelManager) AddDirectoryVolume(
	volumeID domain.VolumeID,
	path string,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if already tracking
	if _, exists := m.collectors[volumeID]; exists {
		return nil
	}

	// Create platform-specific label collector for this directory volume
	collector, err := newDirectoryVolumeLabelCollector(path)
	if err != nil {
		return err
	}

	m.collectors[volumeID] = collector

	// Collect labels immediately
	labels, err := collector.CollectVolumeLabels()
	if err != nil {
		m.log.Warn().
			Err(err).
			Uint16("volume_id", uint16(volumeID)).
			Str("path", path).
			Msg("failed to collect volume labels")
		labels = make(map[string]string)
	}

	m.labels[volumeID] = labels

	// Create watcher for this volume
	watcher := &labelWatcher{
		volumeID:    volumeID,
		subscribers: make(map[*labelSubscriber]struct{}),
	}
	m.watchers[volumeID] = watcher

	m.log.Info().
		Uint16("volume_id", uint16(volumeID)).
		Str("path", path).
		Int("label_count", len(labels)).
		Msg("started volume label collection")

	return nil
}

// RemoveVolume stops label collection for a volume.
func (m *VolumeLabelManager) RemoveVolume(volumeID domain.VolumeID) error {
	m.mu.Lock()
	watcher, exists := m.watchers[volumeID]
	collector := m.collectors[volumeID]
	if !exists {
		m.mu.Unlock()
		return nil
	}
	delete(m.watchers, volumeID)
	delete(m.collectors, volumeID)
	delete(m.labels, volumeID)
	m.mu.Unlock()

	// Cancel all subscribers
	watcher.subMu.Lock()
	for sub := range watcher.subscribers {
		sub.cancel()
		close(sub.ch)
	}
	watcher.subscribers = nil
	watcher.subMu.Unlock()

	// Close collector
	if collector != nil {
		if err := collector.Close(); err != nil {
			m.log.Error().Err(err).Uint16("volume_id", uint16(volumeID)).Msg("failed to close label collector")
		}
	}

	m.log.Info().
		Uint16("volume_id", uint16(volumeID)).
		Msg("stopped volume label collection")

	return nil
}

// ClearVolumes stops label collection for all volumes.
func (m *VolumeLabelManager) ClearVolumes() error {
	m.mu.Lock()
	watchers := make([]*labelWatcher, 0, len(m.watchers))
	collectors := make([]volume.VolumeLabelCollector, 0, len(m.collectors))

	for _, watcher := range m.watchers {
		watchers = append(watchers, watcher)
	}
	for _, collector := range m.collectors {
		collectors = append(collectors, collector)
	}

	m.watchers = make(map[domain.VolumeID]*labelWatcher)
	m.collectors = make(map[domain.VolumeID]volume.VolumeLabelCollector)
	m.labels = make(map[domain.VolumeID]map[string]string)
	m.mu.Unlock()

	// Cancel all watchers
	for _, watcher := range watchers {
		watcher.subMu.Lock()
		for sub := range watcher.subscribers {
			sub.cancel()
			close(sub.ch)
		}
		watcher.subscribers = nil
		watcher.subMu.Unlock()
	}

	// Close all collectors
	for _, collector := range collectors {
		if err := collector.Close(); err != nil {
			m.log.Error().Err(err).Msg("failed to close label collector")
		}
	}

	m.log.Info().Msg("cleared all volume label collection")

	return nil
}

// WatchVolume subscribes to label updates for a volume.
// Returns a channel that receives the current labels immediately,
// and will receive updates if labels are refreshed in the future.
// The channel is closed when the context is canceled or the volume is removed.
func (m *VolumeLabelManager) WatchVolume(ctx context.Context, volumeID domain.VolumeID) (<-chan map[string]string, error) {
	m.mu.RLock()
	watcher, exists := m.watchers[volumeID]
	labels := m.labels[volumeID]
	m.mu.RUnlock()

	if !exists {
		return nil, volumeerrors.ErrNotFound
	}

	// Create subscriber
	ch := make(chan map[string]string, 10) // Buffered to avoid blocking publisher
	subCtx, cancel := context.WithCancel(ctx)
	sub := &labelSubscriber{
		ch:     ch,
		ctx:    subCtx,
		cancel: cancel,
	}

	watcher.subMu.Lock()
	watcher.subscribers[sub] = struct{}{}
	watcher.subMu.Unlock()

	// Send current labels immediately
	if labels != nil {
		labelsCopy := make(map[string]string, len(labels))
		for k, v := range labels {
			labelsCopy[k] = v
		}
		select {
		case ch <- labelsCopy:
		default:
		}
	}

	// Monitor context cancellation
	go func() {
		<-subCtx.Done()
		m.unsubscribe(watcher, sub)
	}()

	return ch, nil
}

func (m *VolumeLabelManager) unsubscribe(watcher *labelWatcher, sub *labelSubscriber) {
	watcher.subMu.Lock()
	defer watcher.subMu.Unlock()

	// Check if subscriber still exists - it may have already been removed
	// and its channel closed by RemoveVolume or ClearVolumes
	if _, exists := watcher.subscribers[sub]; !exists {
		return
	}

	delete(watcher.subscribers, sub)
	close(sub.ch)
}

// RefreshVolume re-collects labels for a volume and notifies watchers.
// This can be used in the future when config-based labels are supported.
func (m *VolumeLabelManager) RefreshVolume(volumeID domain.VolumeID) error {
	m.mu.Lock()
	collector, exists := m.collectors[volumeID]
	watcher := m.watchers[volumeID]
	if !exists {
		m.mu.Unlock()
		return volumeerrors.ErrNotFound
	}

	// Collect labels
	labels, err := collector.CollectVolumeLabels()
	if err != nil {
		m.mu.Unlock()
		return err
	}

	m.labels[volumeID] = labels
	m.mu.Unlock()

	// Notify watchers
	m.publishLabels(watcher, labels)

	m.log.Debug().
		Uint16("volume_id", uint16(volumeID)).
		Int("label_count", len(labels)).
		Msg("refreshed volume labels")

	return nil
}

func (m *VolumeLabelManager) publishLabels(watcher *labelWatcher, labels map[string]string) {
	watcher.subMu.RLock()
	defer watcher.subMu.RUnlock()

	for sub := range watcher.subscribers {
		// Make a copy for each subscriber
		labelsCopy := make(map[string]string, len(labels))
		for k, v := range labels {
			labelsCopy[k] = v
		}

		select {
		case sub.ch <- labelsCopy:
			// Successfully sent
		case <-sub.ctx.Done():
			// Subscriber context canceled, skip
		default:
			// Subscriber channel full, drop this update
			m.log.Warn().
				Uint16("volume_id", uint16(watcher.volumeID)).
				Msg("subscriber channel full, dropping label update")
		}
	}
}
