package volume

import (
	"sync"
	"testing"

	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/port"
	"github.com/amari/mithril/chunk-node/port/volume"
	"github.com/amari/mithril/chunk-node/volumeerrors"
)

// mockVolume implements volume.Volume for testing
type mockVolume struct {
	id     domain.VolumeID
	closed bool
}

func newMockVolume(id domain.VolumeID) *mockVolume {
	return &mockVolume{id: id}
}

func (m *mockVolume) ID() domain.VolumeID {
	return m.id
}

func (m *mockVolume) Close() error {
	m.closed = true
	return nil
}

func (m *mockVolume) Chunks() port.ChunkStore {
	return nil
}

// Verify mockVolume implements volume.Volume
var _ volume.Volume = (*mockVolume)(nil)

func TestNewVolumeManager(t *testing.T) {
	m := NewVolumeManager()

	if m == nil {
		t.Fatal("NewVolumeManager() returned nil")
	}

	if m.volumeMap == nil {
		t.Error("volumeMap should be initialized")
	}

	if len(m.Volumes()) != 0 {
		t.Errorf("new manager should have 0 volumes, got %d", len(m.Volumes()))
	}
}

func TestVolumeManager_AddVolume(t *testing.T) {
	m := NewVolumeManager()
	v := newMockVolume(domain.VolumeID(1))

	m.AddVolume(v)

	volumes := m.Volumes()
	if len(volumes) != 1 {
		t.Fatalf("expected 1 volume, got %d", len(volumes))
	}

	if volumes[0].ID() != domain.VolumeID(1) {
		t.Errorf("volume ID = %v, want 1", volumes[0].ID())
	}
}

func TestVolumeManager_AddVolume_Multiple(t *testing.T) {
	m := NewVolumeManager()

	v1 := newMockVolume(domain.VolumeID(1))
	v2 := newMockVolume(domain.VolumeID(2))
	v3 := newMockVolume(domain.VolumeID(3))

	m.AddVolume(v1)
	m.AddVolume(v2)
	m.AddVolume(v3)

	volumes := m.Volumes()
	if len(volumes) != 3 {
		t.Fatalf("expected 3 volumes, got %d", len(volumes))
	}
}

func TestVolumeManager_Volumes_ReturnsClone(t *testing.T) {
	m := NewVolumeManager()
	v := newMockVolume(domain.VolumeID(1))
	m.AddVolume(v)

	volumes1 := m.Volumes()
	volumes2 := m.Volumes()

	// Modify the returned slice
	volumes1[0] = newMockVolume(domain.VolumeID(99))

	// Original and second call should be unaffected
	if volumes2[0].ID() != domain.VolumeID(1) {
		t.Error("modifying returned slice should not affect other calls")
	}

	// Manager's internal state should be unaffected
	volumes3 := m.Volumes()
	if volumes3[0].ID() != domain.VolumeID(1) {
		t.Error("modifying returned slice should not affect manager state")
	}
}

func TestVolumeManager_VolumeIDs(t *testing.T) {
	m := NewVolumeManager()

	m.AddVolume(newMockVolume(domain.VolumeID(10)))
	m.AddVolume(newMockVolume(domain.VolumeID(20)))
	m.AddVolume(newMockVolume(domain.VolumeID(30)))

	ids := m.VolumeIDs()

	if len(ids) != 3 {
		t.Fatalf("expected 3 IDs, got %d", len(ids))
	}

	// Check all IDs are present (order should match insertion order)
	expectedIDs := []domain.VolumeID{10, 20, 30}
	for i, expected := range expectedIDs {
		if ids[i] != expected {
			t.Errorf("ids[%d] = %v, want %v", i, ids[i], expected)
		}
	}
}

func TestVolumeManager_VolumeIDs_Empty(t *testing.T) {
	m := NewVolumeManager()

	ids := m.VolumeIDs()

	if ids == nil {
		t.Error("VolumeIDs() should return non-nil slice")
	}
	if len(ids) != 0 {
		t.Errorf("expected 0 IDs, got %d", len(ids))
	}
}

func TestVolumeManager_GetVolumeByID_Found(t *testing.T) {
	m := NewVolumeManager()
	v := newMockVolume(domain.VolumeID(42))
	m.AddVolume(v)

	result, err := m.GetVolumeByID(domain.VolumeID(42))

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected volume, got nil")
	}
	if result.ID() != domain.VolumeID(42) {
		t.Errorf("volume ID = %v, want 42", result.ID())
	}
}

func TestVolumeManager_GetVolumeByID_NotFound(t *testing.T) {
	m := NewVolumeManager()
	m.AddVolume(newMockVolume(domain.VolumeID(1)))

	_, err := m.GetVolumeByID(domain.VolumeID(999))

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err != volumeerrors.ErrNotFound {
		t.Errorf("error = %v, want ErrNotFound", err)
	}
}

func TestVolumeManager_GetVolumeByID_EmptyManager(t *testing.T) {
	m := NewVolumeManager()

	_, err := m.GetVolumeByID(domain.VolumeID(1))

	if err != volumeerrors.ErrNotFound {
		t.Errorf("error = %v, want ErrNotFound", err)
	}
}

func TestVolumeManager_RemoveVolume(t *testing.T) {
	m := NewVolumeManager()
	m.AddVolume(newMockVolume(domain.VolumeID(1)))
	m.AddVolume(newMockVolume(domain.VolumeID(2)))
	m.AddVolume(newMockVolume(domain.VolumeID(3)))

	m.RemoveVolume(domain.VolumeID(2))

	volumes := m.Volumes()
	if len(volumes) != 2 {
		t.Fatalf("expected 2 volumes after removal, got %d", len(volumes))
	}

	// Verify volume 2 is gone
	_, err := m.GetVolumeByID(domain.VolumeID(2))
	if err != volumeerrors.ErrNotFound {
		t.Error("removed volume should not be found")
	}

	// Verify others remain
	if _, err := m.GetVolumeByID(domain.VolumeID(1)); err != nil {
		t.Error("volume 1 should still exist")
	}
	if _, err := m.GetVolumeByID(domain.VolumeID(3)); err != nil {
		t.Error("volume 3 should still exist")
	}
}

func TestVolumeManager_RemoveVolume_NotExists(t *testing.T) {
	m := NewVolumeManager()
	m.AddVolume(newMockVolume(domain.VolumeID(1)))

	// Should not panic when removing non-existent volume
	m.RemoveVolume(domain.VolumeID(999))

	// Original volume should still be there
	if len(m.Volumes()) != 1 {
		t.Error("removing non-existent volume should not affect existing volumes")
	}
}

func TestVolumeManager_RemoveVolume_Empty(t *testing.T) {
	m := NewVolumeManager()

	// Should not panic when removing from empty manager
	m.RemoveVolume(domain.VolumeID(1))

	if len(m.Volumes()) != 0 {
		t.Error("manager should still be empty")
	}
}

func TestVolumeManager_RemoveVolume_First(t *testing.T) {
	m := NewVolumeManager()
	m.AddVolume(newMockVolume(domain.VolumeID(1)))
	m.AddVolume(newMockVolume(domain.VolumeID(2)))
	m.AddVolume(newMockVolume(domain.VolumeID(3)))

	m.RemoveVolume(domain.VolumeID(1))

	ids := m.VolumeIDs()
	if len(ids) != 2 {
		t.Fatalf("expected 2 volumes, got %d", len(ids))
	}
	if ids[0] != domain.VolumeID(2) || ids[1] != domain.VolumeID(3) {
		t.Errorf("unexpected volume order after removal: %v", ids)
	}
}

func TestVolumeManager_RemoveVolume_Last(t *testing.T) {
	m := NewVolumeManager()
	m.AddVolume(newMockVolume(domain.VolumeID(1)))
	m.AddVolume(newMockVolume(domain.VolumeID(2)))
	m.AddVolume(newMockVolume(domain.VolumeID(3)))

	m.RemoveVolume(domain.VolumeID(3))

	ids := m.VolumeIDs()
	if len(ids) != 2 {
		t.Fatalf("expected 2 volumes, got %d", len(ids))
	}
	if ids[0] != domain.VolumeID(1) || ids[1] != domain.VolumeID(2) {
		t.Errorf("unexpected volume order after removal: %v", ids)
	}
}

func TestVolumeManager_ClearVolumes(t *testing.T) {
	m := NewVolumeManager()
	m.AddVolume(newMockVolume(domain.VolumeID(1)))
	m.AddVolume(newMockVolume(domain.VolumeID(2)))
	m.AddVolume(newMockVolume(domain.VolumeID(3)))

	m.ClearVolumes()

	if len(m.Volumes()) != 0 {
		t.Errorf("expected 0 volumes after clear, got %d", len(m.Volumes()))
	}
	if len(m.VolumeIDs()) != 0 {
		t.Errorf("expected 0 IDs after clear, got %d", len(m.VolumeIDs()))
	}

	// Verify lookup fails
	_, err := m.GetVolumeByID(domain.VolumeID(1))
	if err != volumeerrors.ErrNotFound {
		t.Error("lookup should fail after clear")
	}
}

func TestVolumeManager_ClearVolumes_Empty(t *testing.T) {
	m := NewVolumeManager()

	// Should not panic
	m.ClearVolumes()

	if len(m.Volumes()) != 0 {
		t.Error("manager should be empty after clear")
	}
}

func TestVolumeManager_ClearVolumes_ThenAdd(t *testing.T) {
	m := NewVolumeManager()
	m.AddVolume(newMockVolume(domain.VolumeID(1)))
	m.ClearVolumes()

	// Should be able to add volumes again
	m.AddVolume(newMockVolume(domain.VolumeID(2)))

	if len(m.Volumes()) != 1 {
		t.Errorf("expected 1 volume after clear and add, got %d", len(m.Volumes()))
	}

	v, err := m.GetVolumeByID(domain.VolumeID(2))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v.ID() != domain.VolumeID(2) {
		t.Errorf("volume ID = %v, want 2", v.ID())
	}
}

func TestVolumeManager_Concurrent_AddAndRead(t *testing.T) {
	m := NewVolumeManager()
	var wg sync.WaitGroup

	// Add volumes concurrently
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			m.AddVolume(newMockVolume(domain.VolumeID(id)))
		}(i)
	}

	// Read concurrently
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = m.Volumes()
			_ = m.VolumeIDs()
		}()
	}

	wg.Wait()

	// Should have all 100 volumes
	if len(m.Volumes()) != 100 {
		t.Errorf("expected 100 volumes, got %d", len(m.Volumes()))
	}
}

func TestVolumeManager_Concurrent_AddAndRemove(t *testing.T) {
	m := NewVolumeManager()

	// Pre-populate with some volumes
	for i := 0; i < 50; i++ {
		m.AddVolume(newMockVolume(domain.VolumeID(i)))
	}

	var wg sync.WaitGroup

	// Add new volumes
	for i := 50; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			m.AddVolume(newMockVolume(domain.VolumeID(id)))
		}(i)
	}

	// Remove some volumes
	for i := 0; i < 25; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			m.RemoveVolume(domain.VolumeID(id))
		}(i)
	}

	wg.Wait()

	// Should have 75 volumes (50 original - 25 removed + 50 added)
	volumes := m.Volumes()
	if len(volumes) != 75 {
		t.Errorf("expected 75 volumes, got %d", len(volumes))
	}
}

func TestVolumeManager_Concurrent_GetByID(t *testing.T) {
	m := NewVolumeManager()

	// Pre-populate
	for i := 0; i < 10; i++ {
		m.AddVolume(newMockVolume(domain.VolumeID(i)))
	}

	var wg sync.WaitGroup

	// Concurrent reads
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_, _ = m.GetVolumeByID(domain.VolumeID(id % 10))
		}(i)
	}

	wg.Wait()
	// Test passes if no race condition detected
}

func TestVolumeManager_AddVolume_SameIDTwice(t *testing.T) {
	m := NewVolumeManager()

	v1 := newMockVolume(domain.VolumeID(1))
	v2 := newMockVolume(domain.VolumeID(1))

	m.AddVolume(v1)
	m.AddVolume(v2)

	// Both should be in the slice
	volumes := m.Volumes()
	if len(volumes) != 2 {
		t.Errorf("expected 2 volumes in slice, got %d", len(volumes))
	}

	// Map should have the second one (overwrites)
	v, err := m.GetVolumeByID(domain.VolumeID(1))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v != v2 {
		t.Error("GetVolumeByID should return the most recently added volume with same ID")
	}
}

func TestVolumeManager_ZeroVolumeID(t *testing.T) {
	m := NewVolumeManager()
	v := newMockVolume(domain.VolumeID(0))

	m.AddVolume(v)

	result, err := m.GetVolumeByID(domain.VolumeID(0))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.ID() != domain.VolumeID(0) {
		t.Errorf("volume ID = %v, want 0", result.ID())
	}
}

func TestVolumeManager_MaxVolumeID(t *testing.T) {
	m := NewVolumeManager()
	maxID := domain.VolumeID(65535) // max uint16
	v := newMockVolume(maxID)

	m.AddVolume(v)

	result, err := m.GetVolumeByID(maxID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.ID() != maxID {
		t.Errorf("volume ID = %v, want %v", result.ID(), maxID)
	}
}

func TestVolumeManager_RemoveVolume_ThenReAdd(t *testing.T) {
	m := NewVolumeManager()
	v1 := newMockVolume(domain.VolumeID(1))

	m.AddVolume(v1)
	m.RemoveVolume(domain.VolumeID(1))

	// Verify removed
	_, err := m.GetVolumeByID(domain.VolumeID(1))
	if err != volumeerrors.ErrNotFound {
		t.Error("volume should be removed")
	}

	// Re-add with same ID
	v2 := newMockVolume(domain.VolumeID(1))
	m.AddVolume(v2)

	// Should be found again
	result, err := m.GetVolumeByID(domain.VolumeID(1))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != v2 {
		t.Error("should return newly added volume")
	}
}

func TestVolumeManager_VolumeIDs_Order(t *testing.T) {
	m := NewVolumeManager()

	// Add in specific order
	m.AddVolume(newMockVolume(domain.VolumeID(5)))
	m.AddVolume(newMockVolume(domain.VolumeID(3)))
	m.AddVolume(newMockVolume(domain.VolumeID(7)))
	m.AddVolume(newMockVolume(domain.VolumeID(1)))

	ids := m.VolumeIDs()

	// Should maintain insertion order
	expected := []domain.VolumeID{5, 3, 7, 1}
	for i, id := range ids {
		if id != expected[i] {
			t.Errorf("ids[%d] = %v, want %v", i, id, expected[i])
		}
	}
}

func TestVolumeManager_Volumes_Order(t *testing.T) {
	m := NewVolumeManager()

	// Add in specific order
	m.AddVolume(newMockVolume(domain.VolumeID(5)))
	m.AddVolume(newMockVolume(domain.VolumeID(3)))
	m.AddVolume(newMockVolume(domain.VolumeID(7)))

	volumes := m.Volumes()

	// Should maintain insertion order
	expected := []domain.VolumeID{5, 3, 7}
	for i, v := range volumes {
		if v.ID() != expected[i] {
			t.Errorf("volumes[%d].ID() = %v, want %v", i, v.ID(), expected[i])
		}
	}
}
