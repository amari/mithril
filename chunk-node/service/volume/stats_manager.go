package volume

import (
	"context"
	"sync"
	"time"

	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/port/volume"
	"github.com/amari/mithril/chunk-node/volumeerrors"
	"github.com/rs/zerolog"
)

// VolumeStatsManager implements VolumeStatsProvider and manages per-volume stats collection.
// It knows how to create platform-specific stats collectors for different volume types
// and manages background polling + publishing stats to watchers.
type VolumeStatsManager struct {
	log *zerolog.Logger

	mu         sync.RWMutex
	collectors map[domain.VolumeID]volumeStatsCollector
	monitors   map[domain.VolumeID]*statsMonitor
}

var _ volume.VolumeStatsProvider = (*VolumeStatsManager)(nil)

type statsMonitor struct {
	volumeID     domain.VolumeID
	collector    volumeStatsCollector
	pollInterval time.Duration
	cancel       context.CancelFunc
	done         chan struct{}

	// Subscriber management
	subMu       sync.RWMutex
	subscribers map[*subscriber]struct{}
}

type subscriber struct {
	ch     chan domain.VolumeStats
	ctx    context.Context
	cancel context.CancelFunc
}

func NewVolumeStatsManager(log *zerolog.Logger) *VolumeStatsManager {
	return &VolumeStatsManager{
		log:        log,
		collectors: make(map[domain.VolumeID]volumeStatsCollector),
		monitors:   make(map[domain.VolumeID]*statsMonitor),
	}
}

// VolumeStats implements VolumeStatsProvider by delegating to the per-volume collector.
func (m *VolumeStatsManager) VolumeStats(ctx context.Context, volumeID domain.VolumeID) (domain.VolumeStats, error) {
	m.mu.RLock()
	collector, exists := m.collectors[volumeID]
	m.mu.RUnlock()

	if !exists {
		return domain.VolumeStats{}, volumeerrors.ErrNotFound
	}

	return collector.CollectStats(ctx)
}

// AddDirectoryVolume creates a stats collector for a directory volume and starts polling.
// "Peeks under the hood" to create platform-specific collector based on the directory path.
func (m *VolumeStatsManager) AddDirectoryVolume(
	volumeID domain.VolumeID,
	path string,
	pollInterval time.Duration,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if already monitoring
	if _, exists := m.collectors[volumeID]; exists {
		return nil
	}

	// Create platform-specific stats collector for this directory volume
	// Collectors start their own poll loops in their constructors
	collector, err := newDirectoryVolumeStatsCollector(
		context.Background(),
		volumeID,
		path,
		pollInterval,
		m.log,
	)
	if err != nil {
		return err
	}

	m.collectors[volumeID] = collector

	// Create monitor context
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	monitor := &statsMonitor{
		volumeID:     volumeID,
		collector:    collector,
		pollInterval: pollInterval,
		cancel:       cancel,
		done:         done,
		subscribers:  make(map[*subscriber]struct{}),
	}

	m.monitors[volumeID] = monitor

	// Start background polling goroutine for publishing to watchers
	go m.pollStats(ctx, monitor)

	m.log.Info().
		Uint16("volume_id", uint16(volumeID)).
		Str("path", path).
		Dur("poll_interval", pollInterval).
		Msg("started volume stats collection")

	return nil
}

// RemoveVolume stops stats collection for a volume.
func (m *VolumeStatsManager) RemoveVolume(volumeID domain.VolumeID) error {
	m.mu.Lock()
	monitor, exists := m.monitors[volumeID]
	collector := m.collectors[volumeID]
	if !exists {
		m.mu.Unlock()
		return nil
	}
	delete(m.monitors, volumeID)
	delete(m.collectors, volumeID)
	m.mu.Unlock()

	// Cancel all subscribers
	monitor.subMu.Lock()
	for sub := range monitor.subscribers {
		sub.cancel()
		close(sub.ch)
	}
	monitor.subscribers = nil
	monitor.subMu.Unlock()

	// Cancel and wait for polling goroutine to finish
	monitor.cancel()
	<-monitor.done

	// Close collector
	if collector != nil {
		if err := collector.Close(); err != nil {
			m.log.Error().Err(err).Uint16("volume_id", uint16(volumeID)).Msg("failed to close stats collector")
		}
	}

	m.log.Info().
		Uint16("volume_id", uint16(volumeID)).
		Msg("stopped volume stats collection")

	return nil
}

// ClearVolumes stops stats collection for all volumes.
func (m *VolumeStatsManager) ClearVolumes() error {
	m.mu.Lock()
	monitors := make([]*statsMonitor, 0, len(m.monitors))
	collectors := make([]volumeStatsCollector, 0, len(m.collectors))

	for _, monitor := range m.monitors {
		monitors = append(monitors, monitor)
	}
	for _, collector := range m.collectors {
		collectors = append(collectors, collector)
	}

	m.monitors = make(map[domain.VolumeID]*statsMonitor)
	m.collectors = make(map[domain.VolumeID]volumeStatsCollector)
	m.mu.Unlock()

	// Cancel all monitors and wait for them to finish
	for _, monitor := range monitors {
		// Cancel all subscribers
		monitor.subMu.Lock()
		for sub := range monitor.subscribers {
			sub.cancel()
			close(sub.ch)
		}
		monitor.subscribers = nil
		monitor.subMu.Unlock()

		// Cancel and wait for polling goroutine
		monitor.cancel()
		<-monitor.done
	}

	// Close all collectors
	for _, collector := range collectors {
		if err := collector.Close(); err != nil {
			m.log.Error().Err(err).Msg("failed to close stats collector")
		}
	}

	m.log.Info().Msg("cleared all volume stats collection")

	return nil
}

// WatchVolume subscribes to stats updates for a volume.
// Returns a channel that receives VolumeStats. The channel is closed when
// the context is canceled or the volume is removed.
func (m *VolumeStatsManager) WatchVolume(ctx context.Context, volumeID domain.VolumeID) (<-chan domain.VolumeStats, error) {
	m.mu.RLock()
	monitor, exists := m.monitors[volumeID]
	m.mu.RUnlock()

	if !exists {
		return nil, volumeerrors.ErrNotFound
	}

	// Create subscriber
	ch := make(chan domain.VolumeStats, 10) // Buffered to avoid blocking publisher
	subCtx, cancel := context.WithCancel(ctx)
	sub := &subscriber{
		ch:     ch,
		ctx:    subCtx,
		cancel: cancel,
	}

	monitor.subMu.Lock()
	monitor.subscribers[sub] = struct{}{}
	monitor.subMu.Unlock()

	// Monitor context cancellation
	go func() {
		<-subCtx.Done()
		m.unsubscribe(monitor, sub)
	}()

	return ch, nil
}

func (m *VolumeStatsManager) unsubscribe(monitor *statsMonitor, sub *subscriber) {
	monitor.subMu.Lock()
	defer monitor.subMu.Unlock()

	delete(monitor.subscribers, sub)
	close(sub.ch)
}

// pollStats is the background goroutine that polls stats and publishes to subscribers
func (m *VolumeStatsManager) pollStats(ctx context.Context, monitor *statsMonitor) {
	defer close(monitor.done)

	ticker := time.NewTicker(monitor.pollInterval)
	defer ticker.Stop()

	// Poll immediately on start
	m.pollAndPublish(ctx, monitor)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.pollAndPublish(ctx, monitor)
		}
	}
}

func (m *VolumeStatsManager) pollAndPublish(ctx context.Context, monitor *statsMonitor) {
	stats, err := monitor.collector.CollectStats(ctx)
	if err != nil {
		m.log.Error().
			Err(err).
			Uint16("volume_id", uint16(monitor.volumeID)).
			Msg("failed to collect volume stats")
		return
	}

	// Publish to all subscribers
	monitor.subMu.RLock()
	defer monitor.subMu.RUnlock()

	for sub := range monitor.subscribers {
		select {
		case sub.ch <- stats:
			// Successfully sent
		case <-sub.ctx.Done():
			// Subscriber context canceled, skip
		default:
			// Subscriber channel full, drop this update
			m.log.Warn().
				Uint16("volume_id", uint16(monitor.volumeID)).
				Msg("subscriber channel full, dropping stats update")
		}
	}
}
