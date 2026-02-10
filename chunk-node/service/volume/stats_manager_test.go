package volume

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/amari/mithril/chunk-node/domain"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
	"github.com/amari/mithril/chunk-node/volumeerrors"
	"github.com/rs/zerolog"
)

// Verify VolumeStatsManager implements VolumeStatsProvider
var _ portvolume.VolumeStatsProvider = (*VolumeStatsManager)(nil)

// mockStatsCollector implements volumeStatsCollector for testing
type mockStatsCollector struct {
	mu       sync.Mutex
	volumeID domain.VolumeID
	stats    domain.VolumeStats
	closed   bool
	closeErr error
}

func newMockStatsCollector(volumeID domain.VolumeID) *mockStatsCollector {
	return &mockStatsCollector{
		volumeID: volumeID,
		stats: domain.VolumeStats{
			VolumeID: volumeID,
			SpaceUtilization: domain.Sample[*domain.SpaceUtilizationStats]{
				Value: &domain.SpaceUtilizationStats{
					TotalBytes: 1000000,
					UsedBytes:  500000,
					FreeBytes:  500000,
				},
				Epoch: 1,
				Time:  time.Now(),
			},
		},
	}
}

func (m *mockStatsCollector) CollectStats(ctx context.Context) (domain.VolumeStats, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stats, nil
}

func (m *mockStatsCollector) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return m.closeErr
}

func (m *mockStatsCollector) isClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

func (m *mockStatsCollector) setStats(stats domain.VolumeStats) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stats = stats
}

func testLogger() *zerolog.Logger {
	logger := zerolog.Nop()
	return &logger
}

func TestNewVolumeStatsManager(t *testing.T) {
	m := NewVolumeStatsManager(testLogger())

	if m == nil {
		t.Fatal("NewVolumeStatsManager() returned nil")
	}

	if m.collectors == nil {
		t.Error("collectors map should be initialized")
	}

	if m.monitors == nil {
		t.Error("monitors map should be initialized")
	}
}

func TestVolumeStatsManager_VolumeStats_NotFound(t *testing.T) {
	m := NewVolumeStatsManager(testLogger())

	_, err := m.VolumeStats(context.Background(), domain.VolumeID(999))

	if err != volumeerrors.ErrNotFound {
		t.Errorf("VolumeStats() error = %v, want ErrNotFound", err)
	}
}

func TestVolumeStatsManager_VolumeStats_WithCollector(t *testing.T) {
	m := NewVolumeStatsManager(testLogger())
	volumeID := domain.VolumeID(1)

	// Manually add a mock collector
	mockCollector := newMockStatsCollector(volumeID)
	m.mu.Lock()
	m.collectors[volumeID] = mockCollector
	m.mu.Unlock()

	stats, err := m.VolumeStats(context.Background(), volumeID)

	if err != nil {
		t.Fatalf("VolumeStats() error = %v", err)
	}

	if stats.VolumeID != volumeID {
		t.Errorf("VolumeID = %v, want %v", stats.VolumeID, volumeID)
	}
}

func TestVolumeStatsManager_RemoveVolume_NotExists(t *testing.T) {
	m := NewVolumeStatsManager(testLogger())

	// Should not error when removing non-existent volume
	err := m.RemoveVolume(domain.VolumeID(999))

	if err != nil {
		t.Errorf("RemoveVolume() error = %v, want nil", err)
	}
}

func TestVolumeStatsManager_RemoveVolume_ClosesCollector(t *testing.T) {
	m := NewVolumeStatsManager(testLogger())
	volumeID := domain.VolumeID(1)

	// Create mock collector and monitor
	mockCollector := newMockStatsCollector(volumeID)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	monitor := &statsMonitor{
		volumeID:     volumeID,
		collector:    mockCollector,
		pollInterval: time.Second,
		cancel:       cancel,
		done:         done,
		subscribers:  make(map[*subscriber]struct{}),
	}

	// Start a mock poll goroutine
	go func() {
		<-ctx.Done()
		close(done)
	}()

	m.mu.Lock()
	m.collectors[volumeID] = mockCollector
	m.monitors[volumeID] = monitor
	m.mu.Unlock()

	err := m.RemoveVolume(volumeID)

	if err != nil {
		t.Fatalf("RemoveVolume() error = %v", err)
	}

	// Collector should be closed
	if !mockCollector.isClosed() {
		t.Error("collector should be closed after RemoveVolume")
	}

	// Should be removed from maps
	m.mu.RLock()
	_, hasCollector := m.collectors[volumeID]
	_, hasMonitor := m.monitors[volumeID]
	m.mu.RUnlock()

	if hasCollector {
		t.Error("collector should be removed from map")
	}
	if hasMonitor {
		t.Error("monitor should be removed from map")
	}
}

func TestVolumeStatsManager_ClearVolumes(t *testing.T) {
	m := NewVolumeStatsManager(testLogger())

	// Add multiple volumes
	for i := 0; i < 3; i++ {
		volumeID := domain.VolumeID(i)
		mockCollector := newMockStatsCollector(volumeID)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})

		monitor := &statsMonitor{
			volumeID:     volumeID,
			collector:    mockCollector,
			pollInterval: time.Second,
			cancel:       cancel,
			done:         done,
			subscribers:  make(map[*subscriber]struct{}),
		}

		go func() {
			<-ctx.Done()
			close(done)
		}()

		m.mu.Lock()
		m.collectors[volumeID] = mockCollector
		m.monitors[volumeID] = monitor
		m.mu.Unlock()
	}

	err := m.ClearVolumes()

	if err != nil {
		t.Fatalf("ClearVolumes() error = %v", err)
	}

	m.mu.RLock()
	numCollectors := len(m.collectors)
	numMonitors := len(m.monitors)
	m.mu.RUnlock()

	if numCollectors != 0 {
		t.Errorf("expected 0 collectors, got %d", numCollectors)
	}
	if numMonitors != 0 {
		t.Errorf("expected 0 monitors, got %d", numMonitors)
	}
}

func TestVolumeStatsManager_ClearVolumes_Empty(t *testing.T) {
	m := NewVolumeStatsManager(testLogger())

	// Should not panic on empty manager
	err := m.ClearVolumes()

	if err != nil {
		t.Errorf("ClearVolumes() error = %v", err)
	}
}

func TestVolumeStatsManager_WatchVolume_NotFound(t *testing.T) {
	m := NewVolumeStatsManager(testLogger())

	_, err := m.WatchVolume(context.Background(), domain.VolumeID(999))

	if err != volumeerrors.ErrNotFound {
		t.Errorf("WatchVolume() error = %v, want ErrNotFound", err)
	}
}

func TestVolumeStatsManager_WatchVolume_ReturnsChannel(t *testing.T) {
	m := NewVolumeStatsManager(testLogger())
	volumeID := domain.VolumeID(1)

	// Create mock collector and monitor
	mockCollector := newMockStatsCollector(volumeID)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	monitor := &statsMonitor{
		volumeID:     volumeID,
		collector:    mockCollector,
		pollInterval: time.Second,
		cancel:       cancel,
		done:         done,
		subscribers:  make(map[*subscriber]struct{}),
	}

	go func() {
		<-ctx.Done()
		close(done)
	}()

	m.mu.Lock()
	m.collectors[volumeID] = mockCollector
	m.monitors[volumeID] = monitor
	m.mu.Unlock()

	ch, err := m.WatchVolume(context.Background(), volumeID)

	if err != nil {
		t.Fatalf("WatchVolume() error = %v", err)
	}

	if ch == nil {
		t.Error("WatchVolume() returned nil channel")
	}

	// Cleanup
	cancel()
	<-done
}

func TestVolumeStatsManager_WatchVolume_MultipleSubscribers(t *testing.T) {
	m := NewVolumeStatsManager(testLogger())
	volumeID := domain.VolumeID(1)

	mockCollector := newMockStatsCollector(volumeID)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	monitor := &statsMonitor{
		volumeID:     volumeID,
		collector:    mockCollector,
		pollInterval: time.Second,
		cancel:       cancel,
		done:         done,
		subscribers:  make(map[*subscriber]struct{}),
	}

	go func() {
		<-ctx.Done()
		close(done)
	}()

	m.mu.Lock()
	m.collectors[volumeID] = mockCollector
	m.monitors[volumeID] = monitor
	m.mu.Unlock()

	// Create multiple subscribers
	ch1, err1 := m.WatchVolume(context.Background(), volumeID)
	ch2, err2 := m.WatchVolume(context.Background(), volumeID)
	ch3, err3 := m.WatchVolume(context.Background(), volumeID)

	if err1 != nil || err2 != nil || err3 != nil {
		t.Fatalf("WatchVolume() errors: %v, %v, %v", err1, err2, err3)
	}

	if ch1 == nil || ch2 == nil || ch3 == nil {
		t.Error("WatchVolume() returned nil channels")
	}

	// Verify all three subscribers are registered
	monitor.subMu.RLock()
	numSubs := len(monitor.subscribers)
	monitor.subMu.RUnlock()

	if numSubs != 3 {
		t.Errorf("expected 3 subscribers, got %d", numSubs)
	}

	// Cleanup
	cancel()
	<-done
}

func TestVolumeStatsManager_WatchVolume_ContextCancellation(t *testing.T) {
	m := NewVolumeStatsManager(testLogger())
	volumeID := domain.VolumeID(1)

	mockCollector := newMockStatsCollector(volumeID)
	monitorCtx, monitorCancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	monitor := &statsMonitor{
		volumeID:     volumeID,
		collector:    mockCollector,
		pollInterval: time.Second,
		cancel:       monitorCancel,
		done:         done,
		subscribers:  make(map[*subscriber]struct{}),
	}

	go func() {
		<-monitorCtx.Done()
		close(done)
	}()

	m.mu.Lock()
	m.collectors[volumeID] = mockCollector
	m.monitors[volumeID] = monitor
	m.mu.Unlock()

	// Create subscriber with cancellable context
	subCtx, subCancel := context.WithCancel(context.Background())

	ch, err := m.WatchVolume(subCtx, volumeID)
	if err != nil {
		t.Fatalf("WatchVolume() error = %v", err)
	}

	// Verify subscriber is registered
	monitor.subMu.RLock()
	numSubsBefore := len(monitor.subscribers)
	monitor.subMu.RUnlock()

	if numSubsBefore != 1 {
		t.Errorf("expected 1 subscriber before cancel, got %d", numSubsBefore)
	}

	// Cancel the subscriber context
	subCancel()

	// Give time for unsubscribe goroutine to run
	time.Sleep(50 * time.Millisecond)

	// Channel should be closed
	select {
	case _, ok := <-ch:
		if ok {
			t.Error("channel should be closed after context cancellation")
		}
	default:
		// Channel might be closed but we can't read from it
	}

	// Cleanup
	monitorCancel()
	<-done
}

func TestVolumeStatsManager_pollAndPublish(t *testing.T) {
	m := NewVolumeStatsManager(testLogger())
	volumeID := domain.VolumeID(1)

	mockCollector := newMockStatsCollector(volumeID)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	monitor := &statsMonitor{
		volumeID:     volumeID,
		collector:    mockCollector,
		pollInterval: time.Second,
		cancel:       cancel,
		done:         make(chan struct{}),
		subscribers:  make(map[*subscriber]struct{}),
	}

	// Add a subscriber
	subCtx, subCancel := context.WithCancel(context.Background())
	ch := make(chan domain.VolumeStats, 10)
	sub := &subscriber{
		ch:     ch,
		ctx:    subCtx,
		cancel: subCancel,
	}
	monitor.subscribers[sub] = struct{}{}

	// Call pollAndPublish
	m.pollAndPublish(ctx, monitor)

	// Check that stats were published
	select {
	case stats := <-ch:
		if stats.VolumeID != volumeID {
			t.Errorf("received stats VolumeID = %v, want %v", stats.VolumeID, volumeID)
		}
	case <-time.After(time.Second):
		t.Error("timeout waiting for stats")
	}
}

func TestVolumeStatsManager_pollAndPublish_DropsWhenFull(t *testing.T) {
	m := NewVolumeStatsManager(testLogger())
	volumeID := domain.VolumeID(1)

	mockCollector := newMockStatsCollector(volumeID)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	monitor := &statsMonitor{
		volumeID:     volumeID,
		collector:    mockCollector,
		pollInterval: time.Second,
		cancel:       cancel,
		done:         make(chan struct{}),
		subscribers:  make(map[*subscriber]struct{}),
	}

	// Add a subscriber with a full channel (unbuffered)
	subCtx, subCancel := context.WithCancel(context.Background())
	ch := make(chan domain.VolumeStats) // Unbuffered - will be full
	sub := &subscriber{
		ch:     ch,
		ctx:    subCtx,
		cancel: subCancel,
	}
	monitor.subscribers[sub] = struct{}{}

	// Call pollAndPublish - should not block even though channel is full
	done := make(chan struct{})
	go func() {
		m.pollAndPublish(ctx, monitor)
		close(done)
	}()

	select {
	case <-done:
		// Good - didn't block
	case <-time.After(time.Second):
		t.Error("pollAndPublish blocked on full channel")
	}
}

func TestVolumeStatsManager_Concurrent_VolumeStats(t *testing.T) {
	m := NewVolumeStatsManager(testLogger())

	// Add multiple collectors
	for i := 0; i < 10; i++ {
		volumeID := domain.VolumeID(i)
		mockCollector := newMockStatsCollector(volumeID)

		m.mu.Lock()
		m.collectors[volumeID] = mockCollector
		m.mu.Unlock()
	}

	var wg sync.WaitGroup

	// Concurrent reads
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_, _ = m.VolumeStats(context.Background(), domain.VolumeID(id%10))
		}(i)
	}

	wg.Wait()
	// Test passes if no race condition
}

func TestVolumeStatsManager_Concurrent_WatchAndRemove(t *testing.T) {
	m := NewVolumeStatsManager(testLogger())
	volumeID := domain.VolumeID(1)

	// Setup
	mockCollector := newMockStatsCollector(volumeID)
	monitorCtx, monitorCancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	monitor := &statsMonitor{
		volumeID:     volumeID,
		collector:    mockCollector,
		pollInterval: time.Second,
		cancel:       monitorCancel,
		done:         done,
		subscribers:  make(map[*subscriber]struct{}),
	}

	go func() {
		<-monitorCtx.Done()
		close(done)
	}()

	m.mu.Lock()
	m.collectors[volumeID] = mockCollector
	m.monitors[volumeID] = monitor
	m.mu.Unlock()

	var wg sync.WaitGroup

	// Concurrent watches
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = m.WatchVolume(context.Background(), volumeID)
		}()
	}

	// Concurrent remove (will only succeed once)
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Millisecond) // Let some watches start
		_ = m.RemoveVolume(volumeID)
	}()

	wg.Wait()
}

func TestVolumeStatsManager_RemoveVolume_CancelsSubscribers(t *testing.T) {
	m := NewVolumeStatsManager(testLogger())
	volumeID := domain.VolumeID(1)

	mockCollector := newMockStatsCollector(volumeID)
	monitorCtx, monitorCancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	monitor := &statsMonitor{
		volumeID:     volumeID,
		collector:    mockCollector,
		pollInterval: time.Second,
		cancel:       monitorCancel,
		done:         done,
		subscribers:  make(map[*subscriber]struct{}),
	}

	go func() {
		<-monitorCtx.Done()
		close(done)
	}()

	m.mu.Lock()
	m.collectors[volumeID] = mockCollector
	m.monitors[volumeID] = monitor
	m.mu.Unlock()

	// Add subscribers
	ch1, _ := m.WatchVolume(context.Background(), volumeID)
	ch2, _ := m.WatchVolume(context.Background(), volumeID)

	// Remove volume - should close subscriber channels
	err := m.RemoveVolume(volumeID)
	if err != nil {
		t.Fatalf("RemoveVolume() error = %v", err)
	}

	// Channels should be closed
	select {
	case _, ok := <-ch1:
		if ok {
			t.Error("ch1 should be closed")
		}
	case <-time.After(time.Second):
		t.Error("timeout waiting for ch1 to close")
	}

	select {
	case _, ok := <-ch2:
		if ok {
			t.Error("ch2 should be closed")
		}
	case <-time.After(time.Second):
		t.Error("timeout waiting for ch2 to close")
	}
}

func TestVolumeStatsManager_ZeroVolumeID(t *testing.T) {
	m := NewVolumeStatsManager(testLogger())
	volumeID := domain.VolumeID(0)

	mockCollector := newMockStatsCollector(volumeID)
	m.mu.Lock()
	m.collectors[volumeID] = mockCollector
	m.mu.Unlock()

	stats, err := m.VolumeStats(context.Background(), volumeID)

	if err != nil {
		t.Fatalf("VolumeStats() error = %v", err)
	}

	if stats.VolumeID != volumeID {
		t.Errorf("VolumeID = %v, want %v", stats.VolumeID, volumeID)
	}
}

func TestVolumeStatsManager_MaxVolumeID(t *testing.T) {
	m := NewVolumeStatsManager(testLogger())
	volumeID := domain.VolumeID(65535) // max uint16

	mockCollector := newMockStatsCollector(volumeID)
	m.mu.Lock()
	m.collectors[volumeID] = mockCollector
	m.mu.Unlock()

	stats, err := m.VolumeStats(context.Background(), volumeID)

	if err != nil {
		t.Fatalf("VolumeStats() error = %v", err)
	}

	if stats.VolumeID != volumeID {
		t.Errorf("VolumeID = %v, want %v", stats.VolumeID, volumeID)
	}
}

func TestStatsMonitor_SubscriberManagement(t *testing.T) {
	monitor := &statsMonitor{
		volumeID:    domain.VolumeID(1),
		subscribers: make(map[*subscriber]struct{}),
	}

	// Add subscribers
	sub1 := &subscriber{ch: make(chan domain.VolumeStats, 1)}
	sub2 := &subscriber{ch: make(chan domain.VolumeStats, 1)}

	monitor.subMu.Lock()
	monitor.subscribers[sub1] = struct{}{}
	monitor.subscribers[sub2] = struct{}{}
	monitor.subMu.Unlock()

	monitor.subMu.RLock()
	count := len(monitor.subscribers)
	monitor.subMu.RUnlock()

	if count != 2 {
		t.Errorf("expected 2 subscribers, got %d", count)
	}

	// Remove one
	monitor.subMu.Lock()
	delete(monitor.subscribers, sub1)
	monitor.subMu.Unlock()

	monitor.subMu.RLock()
	count = len(monitor.subscribers)
	monitor.subMu.RUnlock()

	if count != 1 {
		t.Errorf("expected 1 subscriber after removal, got %d", count)
	}
}

func TestVolumeStatsManager_ClearVolumes_CancelsAllSubscribers(t *testing.T) {
	m := NewVolumeStatsManager(testLogger())

	channels := make([]<-chan domain.VolumeStats, 0)

	// Add multiple volumes with subscribers
	for i := 0; i < 3; i++ {
		volumeID := domain.VolumeID(i)
		mockCollector := newMockStatsCollector(volumeID)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})

		monitor := &statsMonitor{
			volumeID:     volumeID,
			collector:    mockCollector,
			pollInterval: time.Second,
			cancel:       cancel,
			done:         done,
			subscribers:  make(map[*subscriber]struct{}),
		}

		go func() {
			<-ctx.Done()
			close(done)
		}()

		m.mu.Lock()
		m.collectors[volumeID] = mockCollector
		m.monitors[volumeID] = monitor
		m.mu.Unlock()

		// Add subscriber
		ch, _ := m.WatchVolume(context.Background(), volumeID)
		channels = append(channels, ch)
	}

	// Clear all
	err := m.ClearVolumes()
	if err != nil {
		t.Fatalf("ClearVolumes() error = %v", err)
	}

	// All channels should be closed
	for i, ch := range channels {
		select {
		case _, ok := <-ch:
			if ok {
				t.Errorf("channel %d should be closed", i)
			}
		case <-time.After(time.Second):
			t.Errorf("timeout waiting for channel %d to close", i)
		}
	}
}
