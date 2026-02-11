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

// Verify VolumeLabelManager implements VolumeLabelProvider
var _ portvolume.VolumeLabelProvider = (*VolumeLabelManager)(nil)

// mockLabelCollector implements VolumeLabelCollector for testing
type mockLabelCollector struct {
	mu       sync.Mutex
	labels   map[string]string
	closed   bool
	closeErr error
}

func newMockLabelCollector(labels map[string]string) *mockLabelCollector {
	return &mockLabelCollector{
		labels: labels,
	}
}

func (m *mockLabelCollector) CollectVolumeLabels() (map[string]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Return a copy
	result := make(map[string]string, len(m.labels))
	for k, v := range m.labels {
		result[k] = v
	}
	return result, nil
}

func (m *mockLabelCollector) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return m.closeErr
}

func (m *mockLabelCollector) isClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

func (m *mockLabelCollector) setLabels(labels map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.labels = labels
}

func labelTestLogger() *zerolog.Logger {
	logger := zerolog.Nop()
	return &logger
}

func TestNewVolumeLabelManager(t *testing.T) {
	m := NewVolumeLabelManager(labelTestLogger())

	if m == nil {
		t.Fatal("NewVolumeLabelManager() returned nil")
	}

	if m.collectors == nil {
		t.Error("collectors map should be initialized")
	}

	if m.labels == nil {
		t.Error("labels map should be initialized")
	}

	if m.watchers == nil {
		t.Error("watchers map should be initialized")
	}
}

func TestVolumeLabelManager_VolumeLabels_NotFound(t *testing.T) {
	m := NewVolumeLabelManager(labelTestLogger())

	_, err := m.VolumeLabels(context.Background(), domain.VolumeID(999))

	if err != volumeerrors.ErrNotFound {
		t.Errorf("VolumeLabels() error = %v, want ErrNotFound", err)
	}
}

func TestVolumeLabelManager_VolumeLabels_WithCollector(t *testing.T) {
	m := NewVolumeLabelManager(labelTestLogger())

	volumeID := domain.VolumeID(1)
	expectedLabels := map[string]string{
		"nvme":   "true",
		"ssd":    "true",
		"model":  "Test SSD",
		"vendor": "TestVendor",
	}

	// Manually inject a mock collector and labels
	collector := newMockLabelCollector(expectedLabels)
	m.mu.Lock()
	m.collectors[volumeID] = collector
	m.labels[volumeID] = expectedLabels
	m.watchers[volumeID] = &labelWatcher{
		volumeID:    volumeID,
		subscribers: make(map[*labelSubscriber]struct{}),
	}
	m.mu.Unlock()

	labels, err := m.VolumeLabels(context.Background(), volumeID)
	if err != nil {
		t.Fatalf("VolumeLabels() error = %v", err)
	}

	if len(labels) != len(expectedLabels) {
		t.Errorf("VolumeLabels() returned %d labels, want %d", len(labels), len(expectedLabels))
	}

	for k, v := range expectedLabels {
		if labels[k] != v {
			t.Errorf("VolumeLabels()[%q] = %q, want %q", k, labels[k], v)
		}
	}
}

func TestVolumeLabelManager_VolumeLabels_ReturnsCopy(t *testing.T) {
	m := NewVolumeLabelManager(labelTestLogger())

	volumeID := domain.VolumeID(1)
	originalLabels := map[string]string{
		"nvme": "true",
	}

	collector := newMockLabelCollector(originalLabels)
	m.mu.Lock()
	m.collectors[volumeID] = collector
	m.labels[volumeID] = originalLabels
	m.watchers[volumeID] = &labelWatcher{
		volumeID:    volumeID,
		subscribers: make(map[*labelSubscriber]struct{}),
	}
	m.mu.Unlock()

	labels, err := m.VolumeLabels(context.Background(), volumeID)
	if err != nil {
		t.Fatalf("VolumeLabels() error = %v", err)
	}

	// Modify the returned map
	labels["modified"] = "true"

	// Get labels again and verify original is unchanged
	labels2, _ := m.VolumeLabels(context.Background(), volumeID)
	if _, exists := labels2["modified"]; exists {
		t.Error("VolumeLabels() should return a copy, not the original map")
	}
}

func TestVolumeLabelManager_RemoveVolume_NotExists(t *testing.T) {
	m := NewVolumeLabelManager(labelTestLogger())

	// Should not error when removing non-existent volume
	err := m.RemoveVolume(domain.VolumeID(999))
	if err != nil {
		t.Errorf("RemoveVolume() error = %v, want nil", err)
	}
}

func TestVolumeLabelManager_RemoveVolume_ClosesCollector(t *testing.T) {
	m := NewVolumeLabelManager(labelTestLogger())

	volumeID := domain.VolumeID(1)
	collector := newMockLabelCollector(map[string]string{"test": "true"})

	m.mu.Lock()
	m.collectors[volumeID] = collector
	m.labels[volumeID] = map[string]string{"test": "true"}
	m.watchers[volumeID] = &labelWatcher{
		volumeID:    volumeID,
		subscribers: make(map[*labelSubscriber]struct{}),
	}
	m.mu.Unlock()

	err := m.RemoveVolume(volumeID)
	if err != nil {
		t.Fatalf("RemoveVolume() error = %v", err)
	}

	if !collector.isClosed() {
		t.Error("RemoveVolume() should close the collector")
	}

	// Verify volume is removed
	_, err = m.VolumeLabels(context.Background(), volumeID)
	if err != volumeerrors.ErrNotFound {
		t.Errorf("VolumeLabels() after RemoveVolume() error = %v, want ErrNotFound", err)
	}
}

func TestVolumeLabelManager_ClearVolumes(t *testing.T) {
	m := NewVolumeLabelManager(labelTestLogger())

	collectors := make([]*mockLabelCollector, 3)
	for i := 0; i < 3; i++ {
		volumeID := domain.VolumeID(i + 1)
		collectors[i] = newMockLabelCollector(map[string]string{"id": string(rune('a' + i))})

		m.mu.Lock()
		m.collectors[volumeID] = collectors[i]
		m.labels[volumeID] = map[string]string{"id": string(rune('a' + i))}
		m.watchers[volumeID] = &labelWatcher{
			volumeID:    volumeID,
			subscribers: make(map[*labelSubscriber]struct{}),
		}
		m.mu.Unlock()
	}

	err := m.ClearVolumes()
	if err != nil {
		t.Fatalf("ClearVolumes() error = %v", err)
	}

	// Verify all collectors are closed
	for i, c := range collectors {
		if !c.isClosed() {
			t.Errorf("collector[%d] should be closed", i)
		}
	}

	// Verify all volumes are removed
	for i := 0; i < 3; i++ {
		volumeID := domain.VolumeID(i + 1)
		_, err = m.VolumeLabels(context.Background(), volumeID)
		if err != volumeerrors.ErrNotFound {
			t.Errorf("VolumeLabels(%d) after ClearVolumes() error = %v, want ErrNotFound", volumeID, err)
		}
	}
}

func TestVolumeLabelManager_ClearVolumes_Empty(t *testing.T) {
	m := NewVolumeLabelManager(labelTestLogger())

	// Should not error when clearing empty manager
	err := m.ClearVolumes()
	if err != nil {
		t.Errorf("ClearVolumes() error = %v, want nil", err)
	}
}

func TestVolumeLabelManager_WatchVolume_NotFound(t *testing.T) {
	m := NewVolumeLabelManager(labelTestLogger())

	_, err := m.WatchVolume(context.Background(), domain.VolumeID(999))
	if err != volumeerrors.ErrNotFound {
		t.Errorf("WatchVolume() error = %v, want ErrNotFound", err)
	}
}

func TestVolumeLabelManager_WatchVolume_ReturnsChannel(t *testing.T) {
	m := NewVolumeLabelManager(labelTestLogger())

	volumeID := domain.VolumeID(1)
	expectedLabels := map[string]string{"nvme": "true", "ssd": "true"}
	collector := newMockLabelCollector(expectedLabels)

	m.mu.Lock()
	m.collectors[volumeID] = collector
	m.labels[volumeID] = expectedLabels
	m.watchers[volumeID] = &labelWatcher{
		volumeID:    volumeID,
		subscribers: make(map[*labelSubscriber]struct{}),
	}
	m.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := m.WatchVolume(ctx, volumeID)
	if err != nil {
		t.Fatalf("WatchVolume() error = %v", err)
	}

	if ch == nil {
		t.Fatal("WatchVolume() returned nil channel")
	}

	// Should receive initial labels immediately
	select {
	case labels := <-ch:
		if len(labels) != len(expectedLabels) {
			t.Errorf("received %d labels, want %d", len(labels), len(expectedLabels))
		}
		for k, v := range expectedLabels {
			if labels[k] != v {
				t.Errorf("labels[%q] = %q, want %q", k, labels[k], v)
			}
		}
	case <-time.After(time.Second):
		t.Error("timed out waiting for initial labels")
	}
}

func TestVolumeLabelManager_WatchVolume_MultipleSubscribers(t *testing.T) {
	m := NewVolumeLabelManager(labelTestLogger())

	volumeID := domain.VolumeID(1)
	labels := map[string]string{"nvme": "true"}
	collector := newMockLabelCollector(labels)

	m.mu.Lock()
	m.collectors[volumeID] = collector
	m.labels[volumeID] = labels
	m.watchers[volumeID] = &labelWatcher{
		volumeID:    volumeID,
		subscribers: make(map[*labelSubscriber]struct{}),
	}
	m.mu.Unlock()

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	ch1, err := m.WatchVolume(ctx1, volumeID)
	if err != nil {
		t.Fatalf("WatchVolume() #1 error = %v", err)
	}

	ch2, err := m.WatchVolume(ctx2, volumeID)
	if err != nil {
		t.Fatalf("WatchVolume() #2 error = %v", err)
	}

	// Both should receive initial labels
	for i, ch := range []<-chan map[string]string{ch1, ch2} {
		select {
		case received := <-ch:
			if received["nvme"] != "true" {
				t.Errorf("subscriber %d: labels[nvme] = %q, want \"true\"", i+1, received["nvme"])
			}
		case <-time.After(time.Second):
			t.Errorf("subscriber %d: timed out waiting for labels", i+1)
		}
	}
}

func TestVolumeLabelManager_WatchVolume_ContextCancellation(t *testing.T) {
	m := NewVolumeLabelManager(labelTestLogger())

	volumeID := domain.VolumeID(1)
	labels := map[string]string{"nvme": "true"}
	collector := newMockLabelCollector(labels)

	m.mu.Lock()
	m.collectors[volumeID] = collector
	m.labels[volumeID] = labels
	m.watchers[volumeID] = &labelWatcher{
		volumeID:    volumeID,
		subscribers: make(map[*labelSubscriber]struct{}),
	}
	m.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())

	ch, err := m.WatchVolume(ctx, volumeID)
	if err != nil {
		t.Fatalf("WatchVolume() error = %v", err)
	}

	// Drain initial labels
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for initial labels")
	}

	// Cancel context
	cancel()

	// Channel should be closed
	select {
	case _, ok := <-ch:
		if ok {
			// Might receive one more value before close, try again
			select {
			case _, ok := <-ch:
				if ok {
					t.Error("channel should be closed after context cancellation")
				}
			case <-time.After(time.Second):
				t.Error("timed out waiting for channel close")
			}
		}
	case <-time.After(time.Second):
		t.Error("timed out waiting for channel close")
	}
}

func TestVolumeLabelManager_RefreshVolume_NotFound(t *testing.T) {
	m := NewVolumeLabelManager(labelTestLogger())

	err := m.RefreshVolume(domain.VolumeID(999))
	if err != volumeerrors.ErrNotFound {
		t.Errorf("RefreshVolume() error = %v, want ErrNotFound", err)
	}
}

func TestVolumeLabelManager_RefreshVolume_UpdatesLabels(t *testing.T) {
	m := NewVolumeLabelManager(labelTestLogger())

	volumeID := domain.VolumeID(1)
	initialLabels := map[string]string{"nvme": "true"}
	collector := newMockLabelCollector(initialLabels)

	m.mu.Lock()
	m.collectors[volumeID] = collector
	m.labels[volumeID] = initialLabels
	m.watchers[volumeID] = &labelWatcher{
		volumeID:    volumeID,
		subscribers: make(map[*labelSubscriber]struct{}),
	}
	m.mu.Unlock()

	// Update collector's labels
	newLabels := map[string]string{"nvme": "true", "vendor": "NewVendor"}
	collector.setLabels(newLabels)

	err := m.RefreshVolume(volumeID)
	if err != nil {
		t.Fatalf("RefreshVolume() error = %v", err)
	}

	// Get labels and verify they're updated
	labels, err := m.VolumeLabels(context.Background(), volumeID)
	if err != nil {
		t.Fatalf("VolumeLabels() error = %v", err)
	}

	if labels["vendor"] != "NewVendor" {
		t.Errorf("labels[vendor] = %q, want \"NewVendor\"", labels["vendor"])
	}
}

func TestVolumeLabelManager_RefreshVolume_NotifiesWatchers(t *testing.T) {
	m := NewVolumeLabelManager(labelTestLogger())

	volumeID := domain.VolumeID(1)
	initialLabels := map[string]string{"nvme": "true"}
	collector := newMockLabelCollector(initialLabels)

	m.mu.Lock()
	m.collectors[volumeID] = collector
	m.labels[volumeID] = initialLabels
	m.watchers[volumeID] = &labelWatcher{
		volumeID:    volumeID,
		subscribers: make(map[*labelSubscriber]struct{}),
	}
	m.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := m.WatchVolume(ctx, volumeID)
	if err != nil {
		t.Fatalf("WatchVolume() error = %v", err)
	}

	// Drain initial labels
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for initial labels")
	}

	// Update and refresh
	newLabels := map[string]string{"nvme": "true", "model": "NewModel"}
	collector.setLabels(newLabels)

	err = m.RefreshVolume(volumeID)
	if err != nil {
		t.Fatalf("RefreshVolume() error = %v", err)
	}

	// Should receive updated labels
	select {
	case labels := <-ch:
		if labels["model"] != "NewModel" {
			t.Errorf("labels[model] = %q, want \"NewModel\"", labels["model"])
		}
	case <-time.After(time.Second):
		t.Error("timed out waiting for refreshed labels")
	}
}

func TestVolumeLabelManager_RemoveVolume_CancelsSubscribers(t *testing.T) {
	m := NewVolumeLabelManager(labelTestLogger())

	volumeID := domain.VolumeID(1)
	labels := map[string]string{"nvme": "true"}
	collector := newMockLabelCollector(labels)

	m.mu.Lock()
	m.collectors[volumeID] = collector
	m.labels[volumeID] = labels
	m.watchers[volumeID] = &labelWatcher{
		volumeID:    volumeID,
		subscribers: make(map[*labelSubscriber]struct{}),
	}
	m.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := m.WatchVolume(ctx, volumeID)
	if err != nil {
		t.Fatalf("WatchVolume() error = %v", err)
	}

	// Drain initial labels
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for initial labels")
	}

	// Remove volume
	err = m.RemoveVolume(volumeID)
	if err != nil {
		t.Fatalf("RemoveVolume() error = %v", err)
	}

	// Channel should be closed
	select {
	case _, ok := <-ch:
		if ok {
			t.Error("channel should be closed after volume removal")
		}
	case <-time.After(time.Second):
		t.Error("timed out waiting for channel close")
	}
}

func TestVolumeLabelManager_ClearVolumes_CancelsAllSubscribers(t *testing.T) {
	m := NewVolumeLabelManager(labelTestLogger())

	channels := make([]<-chan map[string]string, 3)

	for i := 0; i < 3; i++ {
		volumeID := domain.VolumeID(i + 1)
		labels := map[string]string{"id": string(rune('a' + i))}
		collector := newMockLabelCollector(labels)

		m.mu.Lock()
		m.collectors[volumeID] = collector
		m.labels[volumeID] = labels
		m.watchers[volumeID] = &labelWatcher{
			volumeID:    volumeID,
			subscribers: make(map[*labelSubscriber]struct{}),
		}
		m.mu.Unlock()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ch, err := m.WatchVolume(ctx, volumeID)
		if err != nil {
			t.Fatalf("WatchVolume(%d) error = %v", volumeID, err)
		}
		channels[i] = ch

		// Drain initial labels
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for initial labels for volume %d", volumeID)
		}
	}

	// Clear all volumes
	err := m.ClearVolumes()
	if err != nil {
		t.Fatalf("ClearVolumes() error = %v", err)
	}

	// All channels should be closed
	for i, ch := range channels {
		select {
		case _, ok := <-ch:
			if ok {
				t.Errorf("channel[%d] should be closed after ClearVolumes()", i)
			}
		case <-time.After(time.Second):
			t.Errorf("timed out waiting for channel[%d] close", i)
		}
	}
}

func TestVolumeLabelManager_Concurrent_VolumeLabels(t *testing.T) {
	m := NewVolumeLabelManager(labelTestLogger())

	volumeID := domain.VolumeID(1)
	labels := map[string]string{"nvme": "true", "ssd": "true"}
	collector := newMockLabelCollector(labels)

	m.mu.Lock()
	m.collectors[volumeID] = collector
	m.labels[volumeID] = labels
	m.watchers[volumeID] = &labelWatcher{
		volumeID:    volumeID,
		subscribers: make(map[*labelSubscriber]struct{}),
	}
	m.mu.Unlock()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := m.VolumeLabels(context.Background(), volumeID)
			if err != nil {
				t.Errorf("VolumeLabels() error = %v", err)
			}
		}()
	}
	wg.Wait()
}

func TestVolumeLabelManager_ZeroVolumeID(t *testing.T) {
	m := NewVolumeLabelManager(labelTestLogger())

	volumeID := domain.VolumeID(0)
	labels := map[string]string{"test": "true"}
	collector := newMockLabelCollector(labels)

	m.mu.Lock()
	m.collectors[volumeID] = collector
	m.labels[volumeID] = labels
	m.watchers[volumeID] = &labelWatcher{
		volumeID:    volumeID,
		subscribers: make(map[*labelSubscriber]struct{}),
	}
	m.mu.Unlock()

	got, err := m.VolumeLabels(context.Background(), volumeID)
	if err != nil {
		t.Fatalf("VolumeLabels(0) error = %v", err)
	}

	if got["test"] != "true" {
		t.Errorf("labels[test] = %q, want \"true\"", got["test"])
	}
}

func TestVolumeLabelManager_MaxVolumeID(t *testing.T) {
	m := NewVolumeLabelManager(labelTestLogger())

	volumeID := domain.VolumeID(65535) // max uint16
	labels := map[string]string{"test": "true"}
	collector := newMockLabelCollector(labels)

	m.mu.Lock()
	m.collectors[volumeID] = collector
	m.labels[volumeID] = labels
	m.watchers[volumeID] = &labelWatcher{
		volumeID:    volumeID,
		subscribers: make(map[*labelSubscriber]struct{}),
	}
	m.mu.Unlock()

	got, err := m.VolumeLabels(context.Background(), volumeID)
	if err != nil {
		t.Fatalf("VolumeLabels(65535) error = %v", err)
	}

	if got["test"] != "true" {
		t.Errorf("labels[test] = %q, want \"true\"", got["test"])
	}
}

func TestVolumeLabelManager_EmptyLabels(t *testing.T) {
	m := NewVolumeLabelManager(labelTestLogger())

	volumeID := domain.VolumeID(1)
	labels := map[string]string{}
	collector := newMockLabelCollector(labels)

	m.mu.Lock()
	m.collectors[volumeID] = collector
	m.labels[volumeID] = labels
	m.watchers[volumeID] = &labelWatcher{
		volumeID:    volumeID,
		subscribers: make(map[*labelSubscriber]struct{}),
	}
	m.mu.Unlock()

	got, err := m.VolumeLabels(context.Background(), volumeID)
	if err != nil {
		t.Fatalf("VolumeLabels() error = %v", err)
	}

	if len(got) != 0 {
		t.Errorf("len(labels) = %d, want 0", len(got))
	}
}

func TestLabelWatcher_SubscriberManagement(t *testing.T) {
	m := NewVolumeLabelManager(labelTestLogger())

	volumeID := domain.VolumeID(1)
	labels := map[string]string{"nvme": "true"}
	collector := newMockLabelCollector(labels)

	m.mu.Lock()
	m.collectors[volumeID] = collector
	m.labels[volumeID] = labels
	m.watchers[volumeID] = &labelWatcher{
		volumeID:    volumeID,
		subscribers: make(map[*labelSubscriber]struct{}),
	}
	m.mu.Unlock()

	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	_, err := m.WatchVolume(ctx1, volumeID)
	if err != nil {
		t.Fatalf("WatchVolume() #1 error = %v", err)
	}

	_, err = m.WatchVolume(ctx2, volumeID)
	if err != nil {
		t.Fatalf("WatchVolume() #2 error = %v", err)
	}

	// Check subscriber count
	m.mu.RLock()
	watcher := m.watchers[volumeID]
	m.mu.RUnlock()

	watcher.subMu.RLock()
	count := len(watcher.subscribers)
	watcher.subMu.RUnlock()

	if count != 2 {
		t.Errorf("subscriber count = %d, want 2", count)
	}

	// Cancel first subscriber
	cancel1()
	time.Sleep(50 * time.Millisecond) // Allow goroutine to process cancellation

	watcher.subMu.RLock()
	count = len(watcher.subscribers)
	watcher.subMu.RUnlock()

	if count != 1 {
		t.Errorf("subscriber count after cancel = %d, want 1", count)
	}
}
