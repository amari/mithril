package volume

import (
	"errors"
	"testing"

	"github.com/amari/mithril/chunk-node/domain"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
	"github.com/amari/mithril/chunk-node/volumeerrors"
)

// Verify VolumeAdmissionController implements the interface
var _ portvolume.VolumeAdmissionController = (*VolumeAdmissionController)(nil)

// mockVolumeHealthProvider implements portvolume.VolumeHealthProvider for testing
type mockVolumeHealthProvider struct {
	healthMap map[domain.VolumeID]*domain.VolumeHealth
}

func newMockVolumeHealthProvider() *mockVolumeHealthProvider {
	return &mockVolumeHealthProvider{
		healthMap: make(map[domain.VolumeID]*domain.VolumeHealth),
	}
}

func (m *mockVolumeHealthProvider) GetVolumeHealth(id domain.VolumeID) *domain.VolumeHealth {
	return m.healthMap[id]
}

func (m *mockVolumeHealthProvider) setHealth(id domain.VolumeID, state domain.VolumeState) {
	m.healthMap[id] = &domain.VolumeHealth{State: state}
}

func (m *mockVolumeHealthProvider) setHealthNil(id domain.VolumeID) {
	delete(m.healthMap, id)
}

// --- Constructor Tests ---

func TestNewVolumeAdmissionController(t *testing.T) {
	healthProvider := newMockVolumeHealthProvider()

	controller := NewVolumeAdmissionController(healthProvider)

	if controller == nil {
		t.Fatal("NewVolumeAdmissionController() returned nil")
	}

	if controller.healthProvider != healthProvider {
		t.Error("healthProvider not set correctly")
	}
}

func TestNewVolumeAdmissionController_NilProvider(t *testing.T) {
	controller := NewVolumeAdmissionController(nil)

	if controller == nil {
		t.Fatal("NewVolumeAdmissionController() returned nil")
	}

	if controller.healthProvider != nil {
		t.Error("healthProvider should be nil")
	}
}

// --- Table-Driven Tests ---

func TestVolumeAdmissionController_AdmitRead(t *testing.T) {
	testCases := []struct {
		name      string
		state     *domain.VolumeState // nil means health not found
		wantError bool
		wantErr   error
		wantState domain.VolumeState
	}{
		{"NilHealth", nil, false, nil, 0},
		{"Unknown", ptr(domain.VolumeStateUnknown), false, nil, 0},
		{"OK", ptr(domain.VolumeStateOK), false, nil, 0},
		{"Degraded", ptr(domain.VolumeStateDegraded), false, nil, 0},
		{"Failed", ptr(domain.VolumeStateFailed), true, volumeerrors.ErrFailed, volumeerrors.StateFailed},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			healthProvider := newMockVolumeHealthProvider()
			if tc.state != nil {
				healthProvider.setHealth(1, *tc.state)
			}
			controller := NewVolumeAdmissionController(healthProvider)

			err := controller.AdmitRead(1)

			if tc.wantError {
				if err == nil {
					t.Fatal("AdmitRead() error = nil, want error")
				}
				if !errors.Is(err, tc.wantErr) {
					t.Errorf("AdmitRead() error = %v, want %v", err, tc.wantErr)
				}
				var stateErr *volumeerrors.StateError
				if !errors.As(err, &stateErr) {
					t.Error("error should be a StateError")
				} else if stateErr.State() != tc.wantState {
					t.Errorf("StateError.State() = %v, want %v", stateErr.State(), tc.wantState)
				}
			} else {
				if err != nil {
					t.Errorf("AdmitRead() error = %v, want nil", err)
				}
			}
		})
	}
}

func TestVolumeAdmissionController_AdmitWrite(t *testing.T) {
	testCases := []struct {
		name      string
		state     *domain.VolumeState // nil means health not found
		wantError bool
		wantErr   error
		wantState domain.VolumeState
	}{
		{"NilHealth", nil, false, nil, 0},
		{"Unknown", ptr(domain.VolumeStateUnknown), false, nil, 0},
		{"OK", ptr(domain.VolumeStateOK), false, nil, 0},
		{"Degraded", ptr(domain.VolumeStateDegraded), true, volumeerrors.ErrDegraded, volumeerrors.StateDegraded},
		{"Failed", ptr(domain.VolumeStateFailed), true, volumeerrors.ErrFailed, volumeerrors.StateFailed},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			healthProvider := newMockVolumeHealthProvider()
			if tc.state != nil {
				healthProvider.setHealth(1, *tc.state)
			}
			controller := NewVolumeAdmissionController(healthProvider)

			err := controller.AdmitWrite(1)

			if tc.wantError {
				if err == nil {
					t.Fatal("AdmitWrite() error = nil, want error")
				}
				if !errors.Is(err, tc.wantErr) {
					t.Errorf("AdmitWrite() error = %v, want %v", err, tc.wantErr)
				}
				var stateErr *volumeerrors.StateError
				if !errors.As(err, &stateErr) {
					t.Error("error should be a StateError")
				} else if stateErr.State() != tc.wantState {
					t.Errorf("StateError.State() = %v, want %v", stateErr.State(), tc.wantState)
				}
			} else {
				if err != nil {
					t.Errorf("AdmitWrite() error = %v, want nil", err)
				}
			}
		})
	}
}

func TestVolumeAdmissionController_AdmitStat(t *testing.T) {
	// Stat should always be allowed regardless of state
	testCases := []struct {
		name  string
		state *domain.VolumeState
	}{
		{"NilHealth", nil},
		{"Unknown", ptr(domain.VolumeStateUnknown)},
		{"OK", ptr(domain.VolumeStateOK)},
		{"Degraded", ptr(domain.VolumeStateDegraded)},
		{"Failed", ptr(domain.VolumeStateFailed)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			healthProvider := newMockVolumeHealthProvider()
			if tc.state != nil {
				healthProvider.setHealth(1, *tc.state)
			}
			controller := NewVolumeAdmissionController(healthProvider)

			err := controller.AdmitStat(1)

			if err != nil {
				t.Errorf("AdmitStat() error = %v, want nil", err)
			}
		})
	}
}

// --- Edge Cases ---

func TestVolumeAdmissionController_MultipleVolumes(t *testing.T) {
	healthProvider := newMockVolumeHealthProvider()
	healthProvider.setHealth(1, domain.VolumeStateOK)
	healthProvider.setHealth(2, domain.VolumeStateDegraded)
	healthProvider.setHealth(3, domain.VolumeStateFailed)

	controller := NewVolumeAdmissionController(healthProvider)

	// Volume 1 (OK) - all operations allowed
	if err := controller.AdmitRead(1); err != nil {
		t.Errorf("AdmitRead(1) error = %v, want nil", err)
	}
	if err := controller.AdmitWrite(1); err != nil {
		t.Errorf("AdmitWrite(1) error = %v, want nil", err)
	}
	if err := controller.AdmitStat(1); err != nil {
		t.Errorf("AdmitStat(1) error = %v, want nil", err)
	}

	// Volume 2 (Degraded) - reads and stats allowed, writes denied
	if err := controller.AdmitRead(2); err != nil {
		t.Errorf("AdmitRead(2) error = %v, want nil", err)
	}
	if err := controller.AdmitWrite(2); err == nil {
		t.Error("AdmitWrite(2) error = nil, want error")
	}
	if err := controller.AdmitStat(2); err != nil {
		t.Errorf("AdmitStat(2) error = %v, want nil", err)
	}

	// Volume 3 (Failed) - only stats allowed
	if err := controller.AdmitRead(3); err == nil {
		t.Error("AdmitRead(3) error = nil, want error")
	}
	if err := controller.AdmitWrite(3); err == nil {
		t.Error("AdmitWrite(3) error = nil, want error")
	}
	if err := controller.AdmitStat(3); err != nil {
		t.Errorf("AdmitStat(3) error = %v, want nil", err)
	}
}

func TestVolumeAdmissionController_MaxVolumeID(t *testing.T) {
	healthProvider := newMockVolumeHealthProvider()
	maxID := domain.VolumeID(^uint16(0)) // 65535
	healthProvider.setHealth(maxID, domain.VolumeStateOK)

	controller := NewVolumeAdmissionController(healthProvider)

	if err := controller.AdmitRead(maxID); err != nil {
		t.Errorf("AdmitRead(maxID) error = %v, want nil", err)
	}
	if err := controller.AdmitWrite(maxID); err != nil {
		t.Errorf("AdmitWrite(maxID) error = %v, want nil", err)
	}
	if err := controller.AdmitStat(maxID); err != nil {
		t.Errorf("AdmitStat(maxID) error = %v, want nil", err)
	}
}

func TestVolumeAdmissionController_HealthStateChange(t *testing.T) {
	healthProvider := newMockVolumeHealthProvider()
	healthProvider.setHealth(1, domain.VolumeStateOK)

	controller := NewVolumeAdmissionController(healthProvider)

	// Initially OK
	if err := controller.AdmitWrite(1); err != nil {
		t.Errorf("AdmitWrite() before state change: error = %v, want nil", err)
	}

	// Change to degraded
	healthProvider.setHealth(1, domain.VolumeStateDegraded)

	// Now should fail
	if err := controller.AdmitWrite(1); err == nil {
		t.Error("AdmitWrite() after state change: error = nil, want error")
	}

	// Change back to OK
	healthProvider.setHealth(1, domain.VolumeStateOK)

	// Should succeed again
	if err := controller.AdmitWrite(1); err != nil {
		t.Errorf("AdmitWrite() after recovery: error = %v, want nil", err)
	}
}

func TestVolumeAdmissionController_VolumeRemoved(t *testing.T) {
	healthProvider := newMockVolumeHealthProvider()
	healthProvider.setHealth(1, domain.VolumeStateOK)

	controller := NewVolumeAdmissionController(healthProvider)

	// Initially OK
	if err := controller.AdmitWrite(1); err != nil {
		t.Errorf("AdmitWrite() error = %v, want nil", err)
	}

	// Remove volume (simulate by setting nil)
	healthProvider.setHealthNil(1)

	// Should still succeed (nil health allows operations)
	if err := controller.AdmitWrite(1); err != nil {
		t.Errorf("AdmitWrite() after removal: error = %v, want nil", err)
	}
}

// --- Benchmarks ---

func BenchmarkVolumeAdmissionController_AdmitRead(b *testing.B) {
	healthProvider := newMockVolumeHealthProvider()
	healthProvider.setHealth(1, domain.VolumeStateOK)
	controller := NewVolumeAdmissionController(healthProvider)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = controller.AdmitRead(1)
	}
}

func BenchmarkVolumeAdmissionController_AdmitWrite(b *testing.B) {
	healthProvider := newMockVolumeHealthProvider()
	healthProvider.setHealth(1, domain.VolumeStateOK)
	controller := NewVolumeAdmissionController(healthProvider)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = controller.AdmitWrite(1)
	}
}

func BenchmarkVolumeAdmissionController_AdmitStat(b *testing.B) {
	healthProvider := newMockVolumeHealthProvider()
	healthProvider.setHealth(1, domain.VolumeStateOK)
	controller := NewVolumeAdmissionController(healthProvider)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = controller.AdmitStat(1)
	}
}

func BenchmarkVolumeAdmissionController_AdmitRead_Failed(b *testing.B) {
	healthProvider := newMockVolumeHealthProvider()
	healthProvider.setHealth(1, domain.VolumeStateFailed)
	controller := NewVolumeAdmissionController(healthProvider)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = controller.AdmitRead(1)
	}
}

func BenchmarkVolumeAdmissionController_AdmitWrite_Degraded(b *testing.B) {
	healthProvider := newMockVolumeHealthProvider()
	healthProvider.setHealth(1, domain.VolumeStateDegraded)
	controller := NewVolumeAdmissionController(healthProvider)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = controller.AdmitWrite(1)
	}
}

// --- Helpers ---

func ptr[T any](v T) *T {
	return &v
}
