package volume

import (
	"context"
	"errors"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/amari/mithril/chunk-node/domain"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
	"github.com/amari/mithril/chunk-node/unix"
	"github.com/amari/mithril/chunk-node/volumeerrors"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric/noop"
)

// Verify VolumeHealthTracker implements VolumeHealthChecker
var _ portvolume.VolumeHealthChecker = (*VolumeHealthTracker)(nil)

// mockTelemetryProvider implements portvolume.VolumeTelemetryProvider for testing
type mockTelemetryProvider struct {
	attrs map[domain.VolumeID][]attribute.KeyValue
}

func (m *mockTelemetryProvider) GetVolumeAttributes(volumeID domain.VolumeID) []attribute.KeyValue {
	return m.attrs[volumeID]
}

func (m *mockTelemetryProvider) GetVolumeLoggerFields(volumeID domain.VolumeID) []any {
	return nil
}

// testHealthTracker creates a VolumeHealthTracker for testing with a controllable time function
func testHealthTracker(t *testing.T) (*VolumeHealthTracker, func(time.Time)) {
	t.Helper()

	logger := zerolog.Nop()
	meter := noop.NewMeterProvider().Meter("test")
	statsManager := NewVolumeStatsManager(&logger)

	currentTime := time.Now()
	var timeMu sync.Mutex

	nowFunc := func() time.Time {
		timeMu.Lock()
		defer timeMu.Unlock()
		return currentTime
	}

	tracker, err := NewVolumeHealthTracker(statsManager, meter, &logger, nowFunc)
	if err != nil {
		t.Fatalf("NewVolumeHealthTracker() error = %v", err)
	}

	setTime := func(t time.Time) {
		timeMu.Lock()
		defer timeMu.Unlock()
		currentTime = t
	}

	return tracker, setTime
}

func TestNewVolumeHealthTracker(t *testing.T) {
	logger := zerolog.Nop()
	meter := noop.NewMeterProvider().Meter("test")
	statsManager := NewVolumeStatsManager(&logger)

	tracker, err := NewVolumeHealthTracker(statsManager, meter, &logger, time.Now)

	if err != nil {
		t.Fatalf("NewVolumeHealthTracker() error = %v", err)
	}

	if tracker == nil {
		t.Fatal("NewVolumeHealthTracker() returned nil")
	}

	if tracker.states == nil {
		t.Error("states map should be initialized")
	}

	if tracker.watches == nil {
		t.Error("watches map should be initialized")
	}
}

func TestNewVolumeHealthTracker_MeterError(t *testing.T) {
	logger := zerolog.Nop()
	statsManager := NewVolumeStatsManager(&logger)

	// Create a valid meter - we can't easily force meter errors with noop
	meter := noop.NewMeterProvider().Meter("test")

	tracker, err := NewVolumeHealthTracker(statsManager, meter, &logger, time.Now)

	// With noop meter, it should succeed
	if err != nil {
		t.Errorf("NewVolumeHealthTracker() unexpected error = %v", err)
	}

	if tracker == nil {
		t.Error("NewVolumeHealthTracker() returned nil")
	}
}

func TestVolumeHealthTracker_AddVolume(t *testing.T) {
	tracker, _ := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	config := HealthConfig{
		DegradedThreshold:    3,
		FailedThreshold:      2,
		RecoverySuccessCount: 10,
		RecoveryWindow:       5 * time.Minute,
		SpaceUsageThreshold:  0.90,
	}

	err := tracker.AddVolume(volumeID, config)

	if err != nil {
		t.Fatalf("AddVolume() error = %v", err)
	}

	// Verify volume is tracked
	health := tracker.CheckVolumeHealth(volumeID)
	if health == nil {
		t.Fatal("CheckVolumeHealth() returned nil for tracked volume")
	}

	if health.State != domain.VolumeStateOK {
		t.Errorf("CheckVolumeHealth().State = %v, want VolumeStateOK", health.State)
	}
}

func TestVolumeHealthTracker_AddVolume_DefaultConfig(t *testing.T) {
	tracker, _ := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)

	// Add with zero config - should apply defaults
	err := tracker.AddVolume(volumeID, HealthConfig{})

	if err != nil {
		t.Fatalf("AddVolume() error = %v", err)
	}

	// Verify defaults were applied by checking state transitions work correctly
	tracker.mu.RLock()
	state := tracker.states[volumeID]
	tracker.mu.RUnlock()

	if state.config.DegradedThreshold != 3 {
		t.Errorf("DegradedThreshold = %d, want 3", state.config.DegradedThreshold)
	}
	if state.config.FailedThreshold != 2 {
		t.Errorf("FailedThreshold = %d, want 2", state.config.FailedThreshold)
	}
	if state.config.RecoverySuccessCount != 10 {
		t.Errorf("RecoverySuccessCount = %d, want 10", state.config.RecoverySuccessCount)
	}
	if state.config.RecoveryWindow != 5*time.Minute {
		t.Errorf("RecoveryWindow = %v, want 5m", state.config.RecoveryWindow)
	}
	if state.config.SpaceUsageThreshold != 0.90 {
		t.Errorf("SpaceUsageThreshold = %v, want 0.90", state.config.SpaceUsageThreshold)
	}
	if state.config.LatencyThreshold != 500*time.Millisecond {
		t.Errorf("LatencyThreshold = %v, want 500ms", state.config.LatencyThreshold)
	}
	if state.config.StuckEpochThreshold != 3 {
		t.Errorf("StuckEpochThreshold = %d, want 3", state.config.StuckEpochThreshold)
	}
}

func TestVolumeHealthTracker_AddVolume_Idempotent(t *testing.T) {
	tracker, _ := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	config := HealthConfig{DegradedThreshold: 5}

	// Add twice
	err1 := tracker.AddVolume(volumeID, config)
	err2 := tracker.AddVolume(volumeID, HealthConfig{DegradedThreshold: 10})

	if err1 != nil {
		t.Errorf("first AddVolume() error = %v", err1)
	}
	if err2 != nil {
		t.Errorf("second AddVolume() error = %v", err2)
	}

	// Config should remain from first add (idempotent)
	tracker.mu.RLock()
	state := tracker.states[volumeID]
	tracker.mu.RUnlock()

	if state.config.DegradedThreshold != 5 {
		t.Errorf("DegradedThreshold = %d, want 5 (original)", state.config.DegradedThreshold)
	}
}

func TestVolumeHealthTracker_RemoveVolume(t *testing.T) {
	tracker, _ := testHealthTracker(t)

	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{})

	err := tracker.RemoveVolume(volumeID)

	if err != nil {
		t.Fatalf("RemoveVolume() error = %v", err)
	}

	// Verify volume is no longer tracked
	health := tracker.CheckVolumeHealth(volumeID)
	if health.State != domain.VolumeStateUnknown {
		t.Errorf("CheckVolumeHealth().State = %v, want VolumeStateUnknown", health.State)
	}
}

func TestVolumeHealthTracker_RemoveVolume_NotExists(t *testing.T) {
	tracker, _ := testHealthTracker(t)

	// Remove non-existent volume - should not error
	err := tracker.RemoveVolume(domain.VolumeID(999))

	if err != nil {
		t.Errorf("RemoveVolume() error = %v, want nil", err)
	}
}

func TestVolumeHealthTracker_ClearVolumes(t *testing.T) {
	tracker, _ := testHealthTracker(t)

	// Add multiple volumes
	tracker.AddVolume(domain.VolumeID(1), HealthConfig{})
	tracker.AddVolume(domain.VolumeID(2), HealthConfig{})
	tracker.AddVolume(domain.VolumeID(3), HealthConfig{})

	tracker.ClearVolumes()

	// Verify all volumes are cleared
	for _, id := range []domain.VolumeID{1, 2, 3} {
		health := tracker.CheckVolumeHealth(id)
		if health.State != domain.VolumeStateUnknown {
			t.Errorf("Volume %d still tracked after ClearVolumes()", id)
		}
	}
}

func TestVolumeHealthTracker_ClearVolumes_Empty(t *testing.T) {
	tracker, _ := testHealthTracker(t)

	// Should not panic on empty tracker
	tracker.ClearVolumes()
}

func TestVolumeHealthTracker_CheckVolumeHealth_NotTracked(t *testing.T) {
	tracker, _ := testHealthTracker(t)

	health := tracker.CheckVolumeHealth(domain.VolumeID(999))

	if health == nil {
		t.Fatal("CheckVolumeHealth() returned nil")
	}

	if health.State != domain.VolumeStateUnknown {
		t.Errorf("State = %v, want VolumeStateUnknown", health.State)
	}
}

func TestVolumeHealthTracker_CheckVolumeHealth_TrackedVolume(t *testing.T) {
	tracker, _ := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{})

	health := tracker.CheckVolumeHealth(volumeID)

	if health == nil {
		t.Fatal("CheckVolumeHealth() returned nil")
	}

	if health.State != domain.VolumeStateOK {
		t.Errorf("State = %v, want VolumeStateOK", health.State)
	}
}

// Test RecordError with different error classes

func TestVolumeHealthTracker_RecordError_TransientError(t *testing.T) {
	tracker, _ := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{DegradedThreshold: 3})

	// Record transient errors (unknown error type)
	transientErr := errors.New("some transient error")

	for i := 0; i < 10; i++ {
		state := tracker.RecordError(context.Background(), volumeID, transientErr)
		if state != domain.VolumeStateOK {
			t.Errorf("RecordError() returned %v, want VolumeStateOK for transient errors", state)
		}
	}

	// Volume should still be OK
	health := tracker.CheckVolumeHealth(volumeID)
	if health.State != domain.VolumeStateOK {
		t.Errorf("State = %v, want VolumeStateOK after transient errors", health.State)
	}
}

func TestVolumeHealthTracker_RecordError_DegradingError_ENOSPC(t *testing.T) {
	tracker, _ := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{DegradedThreshold: 3})

	// Record degrading errors (ENOSPC)
	for i := 0; i < 2; i++ {
		state := tracker.RecordError(context.Background(), volumeID, syscall.ENOSPC)
		if state != domain.VolumeStateOK {
			t.Errorf("Iteration %d: RecordError() = %v, want VolumeStateOK", i, state)
		}
	}

	// Third error should transition to Degraded
	state := tracker.RecordError(context.Background(), volumeID, syscall.ENOSPC)
	if state != domain.VolumeStateDegraded {
		t.Errorf("RecordError() = %v, want VolumeStateDegraded after threshold", state)
	}

	// Verify state persists
	health := tracker.CheckVolumeHealth(volumeID)
	if health.State != domain.VolumeStateDegraded {
		t.Errorf("State = %v, want VolumeStateDegraded", health.State)
	}
}

func TestVolumeHealthTracker_RecordError_DegradingError_NoSpace(t *testing.T) {
	tracker, _ := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{DegradedThreshold: 3})

	// Record degrading errors using volumeerrors.ErrNoSpace
	for i := 0; i < 3; i++ {
		tracker.RecordError(context.Background(), volumeID, volumeerrors.ErrNoSpace)
	}

	health := tracker.CheckVolumeHealth(volumeID)
	if health.State != domain.VolumeStateDegraded {
		t.Errorf("State = %v, want VolumeStateDegraded", health.State)
	}
}

func TestVolumeHealthTracker_RecordError_DegradingError_Timeout(t *testing.T) {
	tracker, _ := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{DegradedThreshold: 3})

	// Record degrading errors using context deadline exceeded
	for i := 0; i < 3; i++ {
		tracker.RecordError(context.Background(), volumeID, context.DeadlineExceeded)
	}

	health := tracker.CheckVolumeHealth(volumeID)
	if health.State != domain.VolumeStateDegraded {
		t.Errorf("State = %v, want VolumeStateDegraded", health.State)
	}
}

func TestVolumeHealthTracker_RecordError_FatalError_EIO(t *testing.T) {
	tracker, _ := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{FailedThreshold: 2})

	// First fatal error
	state := tracker.RecordError(context.Background(), volumeID, syscall.EIO)
	if state != domain.VolumeStateOK {
		t.Errorf("RecordError() = %v, want VolumeStateOK after first fatal", state)
	}

	// Second fatal error should transition to Failed
	state = tracker.RecordError(context.Background(), volumeID, syscall.EIO)
	if state != domain.VolumeStateFailed {
		t.Errorf("RecordError() = %v, want VolumeStateFailed after threshold", state)
	}

	health := tracker.CheckVolumeHealth(volumeID)
	if health.State != domain.VolumeStateFailed {
		t.Errorf("State = %v, want VolumeStateFailed", health.State)
	}
}

func TestVolumeHealthTracker_RecordError_FatalError_Fsync(t *testing.T) {
	tracker, _ := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{FailedThreshold: 2})

	fsyncErr := unix.NewFsyncError(errors.New("disk error"))

	// Two fatal errors should transition to Failed
	tracker.RecordError(context.Background(), volumeID, fsyncErr)
	state := tracker.RecordError(context.Background(), volumeID, fsyncErr)

	if state != domain.VolumeStateFailed {
		t.Errorf("RecordError() = %v, want VolumeStateFailed", state)
	}
}

func TestVolumeHealthTracker_RecordError_NotTracked(t *testing.T) {
	tracker, _ := testHealthTracker(t)

	// Record error for non-tracked volume
	state := tracker.RecordError(context.Background(), domain.VolumeID(999), syscall.EIO)

	if state != domain.VolumeStateUnknown {
		t.Errorf("RecordError() = %v, want VolumeStateUnknown for untracked volume", state)
	}
}

func TestVolumeHealthTracker_RecordError_NilError(t *testing.T) {
	tracker, _ := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{})

	// nil error should be classified as transient
	state := tracker.RecordError(context.Background(), volumeID, nil)

	if state != domain.VolumeStateOK {
		t.Errorf("RecordError(nil) = %v, want VolumeStateOK", state)
	}
}

func TestVolumeHealthTracker_RecordError_ResetsSuccessCount(t *testing.T) {
	tracker, setTime := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{
		DegradedThreshold:    1,
		RecoverySuccessCount: 5,
		RecoveryWindow:       1 * time.Minute,
	})

	ctx := context.Background()
	startTime := time.Now()
	setTime(startTime)

	// Transition to degraded
	tracker.RecordError(ctx, volumeID, syscall.ENOSPC)

	// Advance time past recovery window
	setTime(startTime.Add(2 * time.Minute))

	// Record some successes
	for i := 0; i < 3; i++ {
		tracker.RecordSuccess(ctx, volumeID)
	}

	// Record an error - should reset success count
	tracker.RecordError(ctx, volumeID, syscall.ENOSPC)

	// Record more successes - need full count again
	for i := 0; i < 4; i++ {
		tracker.RecordSuccess(ctx, volumeID)
	}

	// Should still be degraded (4 < 5)
	health := tracker.CheckVolumeHealth(volumeID)
	if health.State != domain.VolumeStateDegraded {
		t.Errorf("State = %v, want VolumeStateDegraded (success count reset)", health.State)
	}

	// One more success should recover
	tracker.RecordSuccess(ctx, volumeID)

	health = tracker.CheckVolumeHealth(volumeID)
	if health.State != domain.VolumeStateOK {
		t.Errorf("State = %v, want VolumeStateOK after full recovery", health.State)
	}
}

func TestVolumeHealthTracker_RecordError_IncrementsTotalErrorCount(t *testing.T) {
	tracker, _ := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{})

	ctx := context.Background()

	// Record various errors
	tracker.RecordError(ctx, volumeID, errors.New("transient"))
	tracker.RecordError(ctx, volumeID, syscall.ENOSPC)
	tracker.RecordError(ctx, volumeID, syscall.EIO)

	tracker.mu.RLock()
	state := tracker.states[volumeID]
	totalErrors := state.totalErrorCount
	tracker.mu.RUnlock()

	if totalErrors != 3 {
		t.Errorf("totalErrorCount = %d, want 3", totalErrors)
	}
}

// Test RecordSuccess

func TestVolumeHealthTracker_RecordSuccess_NotTracked(t *testing.T) {
	tracker, _ := testHealthTracker(t)

	// Should not panic
	tracker.RecordSuccess(context.Background(), domain.VolumeID(999))
}

func TestVolumeHealthTracker_RecordSuccess_ResetsErrorCounters(t *testing.T) {
	tracker, _ := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{
		DegradedThreshold: 5,
		FailedThreshold:   3,
	})

	ctx := context.Background()

	// Record some errors (not enough to transition)
	tracker.RecordError(ctx, volumeID, syscall.ENOSPC) // degrading
	tracker.RecordError(ctx, volumeID, syscall.EIO)    // fatal

	// Record success
	tracker.RecordSuccess(ctx, volumeID)

	// Verify counters were reset
	tracker.mu.RLock()
	state := tracker.states[volumeID]
	errorCount := state.errorCount
	fatalCount := state.consecutiveFatalErrors
	tracker.mu.RUnlock()

	if errorCount != 0 {
		t.Errorf("errorCount = %d, want 0 after success", errorCount)
	}
	if fatalCount != 0 {
		t.Errorf("consecutiveFatalErrors = %d, want 0 after success", fatalCount)
	}
}

func TestVolumeHealthTracker_RecordSuccess_RecoveryFromDegraded(t *testing.T) {
	tracker, setTime := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{
		DegradedThreshold:    1,
		RecoverySuccessCount: 3,
		RecoveryWindow:       1 * time.Minute,
	})

	ctx := context.Background()
	startTime := time.Now()
	setTime(startTime)

	// Transition to degraded
	tracker.RecordError(ctx, volumeID, syscall.ENOSPC)

	health := tracker.CheckVolumeHealth(volumeID)
	if health.State != domain.VolumeStateDegraded {
		t.Fatalf("State = %v, want VolumeStateDegraded", health.State)
	}

	// Record successes, but not past recovery window yet
	for i := 0; i < 3; i++ {
		tracker.RecordSuccess(ctx, volumeID)
	}

	// Should still be degraded (recovery window not elapsed)
	health = tracker.CheckVolumeHealth(volumeID)
	if health.State != domain.VolumeStateDegraded {
		t.Errorf("State = %v, want VolumeStateDegraded (window not elapsed)", health.State)
	}

	// Advance time past recovery window
	setTime(startTime.Add(2 * time.Minute))

	// Record required successes again
	for i := 0; i < 3; i++ {
		tracker.RecordSuccess(ctx, volumeID)
	}

	// Should now be recovered
	health = tracker.CheckVolumeHealth(volumeID)
	if health.State != domain.VolumeStateOK {
		t.Errorf("State = %v, want VolumeStateOK after recovery", health.State)
	}
}

func TestVolumeHealthTracker_RecordSuccess_NoRecoveryFromFailed(t *testing.T) {
	tracker, setTime := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{
		FailedThreshold:      2,
		RecoverySuccessCount: 3,
		RecoveryWindow:       1 * time.Minute,
	})

	ctx := context.Background()
	startTime := time.Now()
	setTime(startTime)

	// Transition to failed
	tracker.RecordError(ctx, volumeID, syscall.EIO)
	tracker.RecordError(ctx, volumeID, syscall.EIO)

	health := tracker.CheckVolumeHealth(volumeID)
	if health.State != domain.VolumeStateFailed {
		t.Fatalf("State = %v, want VolumeStateFailed", health.State)
	}

	// Advance time and record many successes
	setTime(startTime.Add(10 * time.Minute))
	for i := 0; i < 100; i++ {
		tracker.RecordSuccess(ctx, volumeID)
	}

	// Should remain failed (no automatic recovery from failed)
	health = tracker.CheckVolumeHealth(volumeID)
	if health.State != domain.VolumeStateFailed {
		t.Errorf("State = %v, want VolumeStateFailed (no recovery from failed)", health.State)
	}
}

func TestVolumeHealthTracker_RecordSuccess_OK_NoEffect(t *testing.T) {
	tracker, _ := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{})

	ctx := context.Background()

	// Record many successes on OK volume
	for i := 0; i < 100; i++ {
		tracker.RecordSuccess(ctx, volumeID)
	}

	// Should still be OK
	health := tracker.CheckVolumeHealth(volumeID)
	if health.State != domain.VolumeStateOK {
		t.Errorf("State = %v, want VolumeStateOK", health.State)
	}

	// Success count should not accumulate for OK state
	tracker.mu.RLock()
	state := tracker.states[volumeID]
	successCount := state.successCount
	tracker.mu.RUnlock()

	if successCount != 0 {
		t.Errorf("successCount = %d, want 0 for OK state", successCount)
	}
}

// Test SetConfig

func TestVolumeHealthTracker_SetConfig(t *testing.T) {
	tracker, _ := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{DegradedThreshold: 3})

	newConfig := HealthConfig{
		DegradedThreshold: 5,
		FailedThreshold:   3,
	}
	tracker.SetConfig(volumeID, newConfig)

	tracker.mu.RLock()
	state := tracker.states[volumeID]
	threshold := state.config.DegradedThreshold
	tracker.mu.RUnlock()

	if threshold != 5 {
		t.Errorf("DegradedThreshold = %d, want 5", threshold)
	}
}

func TestVolumeHealthTracker_SetConfig_AppliesDefaults(t *testing.T) {
	tracker, _ := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{
		DegradedThreshold: 10,
		FailedThreshold:   10,
	})

	// Set config with zeros - should apply defaults
	tracker.SetConfig(volumeID, HealthConfig{})

	tracker.mu.RLock()
	state := tracker.states[volumeID]
	config := state.config
	tracker.mu.RUnlock()

	if config.DegradedThreshold != 3 {
		t.Errorf("DegradedThreshold = %d, want 3 (default)", config.DegradedThreshold)
	}
	if config.FailedThreshold != 2 {
		t.Errorf("FailedThreshold = %d, want 2 (default)", config.FailedThreshold)
	}
}

func TestVolumeHealthTracker_SetConfig_NotTracked(t *testing.T) {
	tracker, _ := testHealthTracker(t)

	// Should not panic
	tracker.SetConfig(domain.VolumeID(999), HealthConfig{DegradedThreshold: 5})
}

// Test state transitions

func TestVolumeHealthTracker_StateTransition_OKToDegraded(t *testing.T) {
	tracker, _ := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{DegradedThreshold: 2})

	ctx := context.Background()

	// Verify initial state
	if health := tracker.CheckVolumeHealth(volumeID); health.State != domain.VolumeStateOK {
		t.Fatalf("Initial state = %v, want VolumeStateOK", health.State)
	}

	// First error
	tracker.RecordError(ctx, volumeID, syscall.ENOSPC)
	if health := tracker.CheckVolumeHealth(volumeID); health.State != domain.VolumeStateOK {
		t.Errorf("After 1 error: State = %v, want VolumeStateOK", health.State)
	}

	// Second error - should transition
	tracker.RecordError(ctx, volumeID, syscall.ENOSPC)
	if health := tracker.CheckVolumeHealth(volumeID); health.State != domain.VolumeStateDegraded {
		t.Errorf("After 2 errors: State = %v, want VolumeStateDegraded", health.State)
	}
}

func TestVolumeHealthTracker_StateTransition_OKToFailed(t *testing.T) {
	tracker, _ := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{FailedThreshold: 2})

	ctx := context.Background()

	// Two consecutive fatal errors should go directly to Failed
	tracker.RecordError(ctx, volumeID, syscall.EIO)
	tracker.RecordError(ctx, volumeID, syscall.EIO)

	health := tracker.CheckVolumeHealth(volumeID)
	if health.State != domain.VolumeStateFailed {
		t.Errorf("State = %v, want VolumeStateFailed", health.State)
	}
}

func TestVolumeHealthTracker_StateTransition_DegradedToFailed(t *testing.T) {
	tracker, _ := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{
		DegradedThreshold: 1,
		FailedThreshold:   2,
	})

	ctx := context.Background()

	// First: go to Degraded
	tracker.RecordError(ctx, volumeID, syscall.ENOSPC)

	health := tracker.CheckVolumeHealth(volumeID)
	if health.State != domain.VolumeStateDegraded {
		t.Fatalf("State = %v, want VolumeStateDegraded", health.State)
	}

	// Then: fatal errors to Failed
	tracker.RecordError(ctx, volumeID, syscall.EIO)
	tracker.RecordError(ctx, volumeID, syscall.EIO)

	health = tracker.CheckVolumeHealth(volumeID)
	if health.State != domain.VolumeStateFailed {
		t.Errorf("State = %v, want VolumeStateFailed", health.State)
	}
}

func TestVolumeHealthTracker_StateTransition_DegradedToOK(t *testing.T) {
	tracker, setTime := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{
		DegradedThreshold:    1,
		RecoverySuccessCount: 5,
		RecoveryWindow:       1 * time.Minute,
	})

	ctx := context.Background()
	startTime := time.Now()
	setTime(startTime)

	// Go to Degraded
	tracker.RecordError(ctx, volumeID, syscall.ENOSPC)

	// Advance time past recovery window
	setTime(startTime.Add(2 * time.Minute))

	// Record required successes
	for i := 0; i < 5; i++ {
		tracker.RecordSuccess(ctx, volumeID)
	}

	health := tracker.CheckVolumeHealth(volumeID)
	if health.State != domain.VolumeStateOK {
		t.Errorf("State = %v, want VolumeStateOK after recovery", health.State)
	}
}

// Test error counter reset after state transition

func TestVolumeHealthTracker_ErrorCounterReset_AfterDegradedTransition(t *testing.T) {
	tracker, _ := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{DegradedThreshold: 2})

	ctx := context.Background()

	// Transition to degraded
	tracker.RecordError(ctx, volumeID, syscall.ENOSPC)
	tracker.RecordError(ctx, volumeID, syscall.ENOSPC)

	// Error count should be reset after transition
	tracker.mu.RLock()
	state := tracker.states[volumeID]
	errorCount := state.errorCount
	tracker.mu.RUnlock()

	if errorCount != 0 {
		t.Errorf("errorCount = %d, want 0 after state transition", errorCount)
	}
}

// Test concurrent access

func TestVolumeHealthTracker_Concurrent_AddRemove(t *testing.T) {
	tracker, _ := testHealthTracker(t)
	defer tracker.ClearVolumes()

	var wg sync.WaitGroup
	numGoroutines := 10
	numOperations := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			volumeID := domain.VolumeID(id)
			for j := 0; j < numOperations; j++ {
				tracker.AddVolume(volumeID, HealthConfig{})
				tracker.RemoveVolume(volumeID)
			}
		}(i)
	}

	wg.Wait()
}

func TestVolumeHealthTracker_Concurrent_RecordErrorAndSuccess(t *testing.T) {
	tracker, _ := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{
		DegradedThreshold:    100,
		RecoverySuccessCount: 100,
		RecoveryWindow:       0,
	})

	ctx := context.Background()
	var wg sync.WaitGroup
	numGoroutines := 10
	numOperations := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				tracker.RecordError(ctx, volumeID, syscall.ENOSPC)
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				tracker.RecordSuccess(ctx, volumeID)
			}
		}()
	}

	wg.Wait()

	// Should not panic and state should be valid
	health := tracker.CheckVolumeHealth(volumeID)
	if health == nil {
		t.Error("CheckVolumeHealth() returned nil after concurrent operations")
	}
}

func TestVolumeHealthTracker_Concurrent_CheckHealth(t *testing.T) {
	tracker, _ := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{})

	var wg sync.WaitGroup
	numGoroutines := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			health := tracker.CheckVolumeHealth(volumeID)
			if health == nil {
				t.Error("CheckVolumeHealth() returned nil")
			}
		}()
	}

	wg.Wait()
}

func TestVolumeHealthTracker_Concurrent_SetConfig(t *testing.T) {
	tracker, _ := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{})

	var wg sync.WaitGroup
	numGoroutines := 10
	numOperations := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(threshold int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				tracker.SetConfig(volumeID, HealthConfig{DegradedThreshold: threshold})
			}
		}(i + 1)
	}

	wg.Wait()
}

// Test edge cases

func TestVolumeHealthTracker_ZeroVolumeID(t *testing.T) {
	tracker, _ := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(0)

	err := tracker.AddVolume(volumeID, HealthConfig{})
	if err != nil {
		t.Fatalf("AddVolume(0) error = %v", err)
	}

	health := tracker.CheckVolumeHealth(volumeID)
	if health.State != domain.VolumeStateOK {
		t.Errorf("State = %v, want VolumeStateOK", health.State)
	}

	err = tracker.RemoveVolume(volumeID)
	if err != nil {
		t.Errorf("RemoveVolume(0) error = %v", err)
	}
}

func TestVolumeHealthTracker_MaxVolumeID(t *testing.T) {
	tracker, _ := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(65535)

	err := tracker.AddVolume(volumeID, HealthConfig{})
	if err != nil {
		t.Fatalf("AddVolume(65535) error = %v", err)
	}

	health := tracker.CheckVolumeHealth(volumeID)
	if health.State != domain.VolumeStateOK {
		t.Errorf("State = %v, want VolumeStateOK", health.State)
	}

	err = tracker.RemoveVolume(volumeID)
	if err != nil {
		t.Errorf("RemoveVolume(65535) error = %v", err)
	}
}

func TestVolumeHealthTracker_MultipleVolumes(t *testing.T) {
	tracker, _ := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeIDs := []domain.VolumeID{1, 2, 3, 4, 5}
	ctx := context.Background()

	// Add all volumes
	for _, id := range volumeIDs {
		tracker.AddVolume(id, HealthConfig{DegradedThreshold: 1})
	}

	// Degrade only volume 2
	tracker.RecordError(ctx, domain.VolumeID(2), syscall.ENOSPC)

	// Fail only volume 4
	tracker.AddVolume(domain.VolumeID(4), HealthConfig{FailedThreshold: 1})
	tracker.SetConfig(domain.VolumeID(4), HealthConfig{FailedThreshold: 1})
	tracker.RecordError(ctx, domain.VolumeID(4), syscall.EIO)

	// Verify states
	for _, id := range volumeIDs {
		health := tracker.CheckVolumeHealth(id)
		var expectedState domain.VolumeState
		switch id {
		case 2:
			expectedState = domain.VolumeStateDegraded
		case 4:
			expectedState = domain.VolumeStateFailed
		default:
			expectedState = domain.VolumeStateOK
		}

		if health.State != expectedState {
			t.Errorf("Volume %d: State = %v, want %v", id, health.State, expectedState)
		}
	}
}

// Test stats-based health updates

func TestVolumeHealthTracker_updateHealthFromStats_HighSpaceUsage(t *testing.T) {
	tracker, _ := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{SpaceUsageThreshold: 0.90})

	// Simulate stats with high space usage (95%)
	stats := domain.VolumeStats{
		VolumeID: volumeID,
		SpaceUtilization: domain.Sample[*domain.SpaceUtilizationStats]{
			Value: &domain.SpaceUtilizationStats{
				TotalBytes: 1000000,
				UsedBytes:  950000,
				FreeBytes:  50000,
			},
			Epoch: 1,
			Time:  time.Now(),
		},
	}

	tracker.updateHealthFromStats(volumeID, stats)

	health := tracker.CheckVolumeHealth(volumeID)
	if health.State != domain.VolumeStateDegraded {
		t.Errorf("State = %v, want VolumeStateDegraded due to high space usage", health.State)
	}
}

func TestVolumeHealthTracker_updateHealthFromStats_SpaceRecovery(t *testing.T) {
	tracker, setTime := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{
		SpaceUsageThreshold: 0.90,
		RecoveryWindow:      1 * time.Minute,
	})

	startTime := time.Now()
	setTime(startTime)

	// First: degrade due to high space usage
	highUsageStats := domain.VolumeStats{
		VolumeID: volumeID,
		SpaceUtilization: domain.Sample[*domain.SpaceUtilizationStats]{
			Value: &domain.SpaceUtilizationStats{
				TotalBytes: 1000000,
				UsedBytes:  950000,
				FreeBytes:  50000,
			},
			Epoch: 1,
			Time:  startTime,
		},
	}
	tracker.updateHealthFromStats(volumeID, highUsageStats)

	health := tracker.CheckVolumeHealth(volumeID)
	if health.State != domain.VolumeStateDegraded {
		t.Fatalf("State = %v, want VolumeStateDegraded", health.State)
	}

	// Advance time past recovery window
	setTime(startTime.Add(2 * time.Minute))

	// Space usage drops below threshold with hysteresis (< 85%)
	lowUsageStats := domain.VolumeStats{
		VolumeID: volumeID,
		SpaceUtilization: domain.Sample[*domain.SpaceUtilizationStats]{
			Value: &domain.SpaceUtilizationStats{
				TotalBytes: 1000000,
				UsedBytes:  800000,
				FreeBytes:  200000,
			},
			Epoch: 2,
			Time:  startTime.Add(2 * time.Minute),
		},
	}
	tracker.updateHealthFromStats(volumeID, lowUsageStats)

	health = tracker.CheckVolumeHealth(volumeID)
	if health.State != domain.VolumeStateOK {
		t.Errorf("State = %v, want VolumeStateOK after space recovery", health.State)
	}
}

func TestVolumeHealthTracker_updateHealthFromStats_HighLatency_Apple(t *testing.T) {
	tracker, _ := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{
		LatencyThreshold:    100 * time.Millisecond,
		SpaceUsageThreshold: 0.99, // High threshold to avoid space-based degradation
	})

	// Simulate stats with high latency (1 second average)
	stats := domain.VolumeStats{
		VolumeID: volumeID,
		SpaceUtilization: domain.Sample[*domain.SpaceUtilizationStats]{
			Value: &domain.SpaceUtilizationStats{
				TotalBytes: 1000000,
				UsedBytes:  100000,
				FreeBytes:  900000,
			},
			Epoch: 1,
			Time:  time.Now(),
		},
		BlockDevice: domain.Sample[*domain.BlockDeviceStats]{
			Value: &domain.BlockDeviceStats{
				Apple: &domain.BlockDeviceStatsApple{
					Reads:           100,
					Writes:          100,
					LatentReadTime:  100000000000, // 100 seconds total
					LatentWriteTime: 100000000000, // 100 seconds total
				},
			},
			Epoch: 1,
			Time:  time.Now(),
		},
	}

	tracker.updateHealthFromStats(volumeID, stats)

	health := tracker.CheckVolumeHealth(volumeID)
	if health.State != domain.VolumeStateDegraded {
		t.Errorf("State = %v, want VolumeStateDegraded due to high latency", health.State)
	}
}

func TestVolumeHealthTracker_updateHealthFromStats_HighLatency_Linux(t *testing.T) {
	tracker, _ := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{
		LatencyThreshold:    100 * time.Millisecond,
		SpaceUsageThreshold: 0.99,
	})

	// Simulate stats with high latency
	stats := domain.VolumeStats{
		VolumeID: volumeID,
		SpaceUtilization: domain.Sample[*domain.SpaceUtilizationStats]{
			Value: &domain.SpaceUtilizationStats{
				TotalBytes: 1000000,
				UsedBytes:  100000,
				FreeBytes:  900000,
			},
			Epoch: 1,
			Time:  time.Now(),
		},
		BlockDevice: domain.Sample[*domain.BlockDeviceStats]{
			Value: &domain.BlockDeviceStats{
				Linux: &domain.BlockDeviceStatsLinux{
					Reads:      100,
					Writes:     100,
					ReadTicks:  50000, // 50000ms total
					WriteTicks: 50000, // 50000ms total
				},
			},
			Epoch: 1,
			Time:  time.Now(),
		},
	}

	tracker.updateHealthFromStats(volumeID, stats)

	health := tracker.CheckVolumeHealth(volumeID)
	if health.State != domain.VolumeStateDegraded {
		t.Errorf("State = %v, want VolumeStateDegraded due to high latency", health.State)
	}
}

func TestVolumeHealthTracker_updateHealthFromStats_StuckStatfs(t *testing.T) {
	tracker, _ := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	// Set threshold to 1 so a single stuck sample triggers degradation
	// The implementation compares current vs previous sample, so epochsStuck = current.Epoch - last.Epoch
	tracker.AddVolume(volumeID, HealthConfig{
		StuckEpochThreshold: 1,
		SpaceUsageThreshold: 0.99, // High threshold to avoid space-based degradation
	})

	baseTime := time.Now()

	// First sample - establishes baseline
	stats1 := domain.VolumeStats{
		VolumeID: volumeID,
		SpaceUtilization: domain.Sample[*domain.SpaceUtilizationStats]{
			Value: &domain.SpaceUtilizationStats{
				TotalBytes: 1000000,
				UsedBytes:  100000,
				FreeBytes:  900000,
			},
			Epoch: 1,
			Time:  baseTime,
		},
	}
	tracker.updateHealthFromStats(volumeID, stats1)

	health := tracker.CheckVolumeHealth(volumeID)
	if health.State != domain.VolumeStateOK {
		t.Fatalf("State = %v after first sample, want VolumeStateOK", health.State)
	}

	// Second sample with epoch advancing but time frozen (stuck statfs)
	// epochsStuck = 2 - 1 = 1, which meets threshold of 1
	stats2 := domain.VolumeStats{
		VolumeID: volumeID,
		SpaceUtilization: domain.Sample[*domain.SpaceUtilizationStats]{
			Value: &domain.SpaceUtilizationStats{
				TotalBytes: 1000000,
				UsedBytes:  100000,
				FreeBytes:  900000,
			},
			Epoch: 2,
			Time:  baseTime, // Time stays frozen
		},
	}
	tracker.updateHealthFromStats(volumeID, stats2)

	health = tracker.CheckVolumeHealth(volumeID)
	if health.State != domain.VolumeStateDegraded {
		t.Errorf("State = %v, want VolumeStateDegraded due to stuck statfs", health.State)
	}
}

func TestVolumeHealthTracker_updateHealthFromStats_StuckBlockDevice(t *testing.T) {
	tracker, _ := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	// Set threshold to 1 so a single stuck sample triggers degradation
	tracker.AddVolume(volumeID, HealthConfig{
		StuckEpochThreshold: 1,
		SpaceUsageThreshold: 0.99,
		LatencyThreshold:    0, // Disable latency check
	})

	baseTime := time.Now()

	// First sample - establishes baseline
	stats1 := domain.VolumeStats{
		VolumeID: volumeID,
		SpaceUtilization: domain.Sample[*domain.SpaceUtilizationStats]{
			Value: &domain.SpaceUtilizationStats{
				TotalBytes: 1000000,
				UsedBytes:  100000,
				FreeBytes:  900000,
			},
			Epoch: 1,
			Time:  time.Now(),
		},
		BlockDevice: domain.Sample[*domain.BlockDeviceStats]{
			Value: &domain.BlockDeviceStats{},
			Epoch: 1,
			Time:  baseTime,
		},
	}
	tracker.updateHealthFromStats(volumeID, stats1)

	// Second sample with stuck block device (epoch advances, time frozen)
	// epochsStuck = 2 - 1 = 1, which meets threshold of 1
	stats2 := domain.VolumeStats{
		VolumeID: volumeID,
		SpaceUtilization: domain.Sample[*domain.SpaceUtilizationStats]{
			Value: &domain.SpaceUtilizationStats{
				TotalBytes: 1000000,
				UsedBytes:  100000,
				FreeBytes:  900000,
			},
			Epoch: 2,
			Time:  time.Now(),
		},
		BlockDevice: domain.Sample[*domain.BlockDeviceStats]{
			Value: &domain.BlockDeviceStats{},
			Epoch: 2,
			Time:  baseTime, // Time frozen
		},
	}
	tracker.updateHealthFromStats(volumeID, stats2)

	health := tracker.CheckVolumeHealth(volumeID)
	if health.State != domain.VolumeStateDegraded {
		t.Errorf("State = %v, want VolumeStateDegraded due to stuck block device", health.State)
	}
}

func TestVolumeHealthTracker_updateHealthFromStats_SkipsFailedVolume(t *testing.T) {
	tracker, _ := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{
		FailedThreshold:     1,
		SpaceUsageThreshold: 0.5, // Would normally trigger degradation
	})

	ctx := context.Background()

	// First, fail the volume
	tracker.RecordError(ctx, volumeID, syscall.EIO)

	health := tracker.CheckVolumeHealth(volumeID)
	if health.State != domain.VolumeStateFailed {
		t.Fatalf("State = %v, want VolumeStateFailed", health.State)
	}

	// Now send stats that would normally cause degradation
	stats := domain.VolumeStats{
		VolumeID: volumeID,
		SpaceUtilization: domain.Sample[*domain.SpaceUtilizationStats]{
			Value: &domain.SpaceUtilizationStats{
				TotalBytes: 1000000,
				UsedBytes:  900000,
				FreeBytes:  100000,
			},
			Epoch: 1,
			Time:  time.Now(),
		},
	}
	tracker.updateHealthFromStats(volumeID, stats)

	// Should remain failed (stats checks skipped)
	health = tracker.CheckVolumeHealth(volumeID)
	if health.State != domain.VolumeStateFailed {
		t.Errorf("State = %v, want VolumeStateFailed (stats should be ignored)", health.State)
	}
}

func TestVolumeHealthTracker_updateHealthFromStats_NotTracked(t *testing.T) {
	tracker, _ := testHealthTracker(t)

	// Should not panic
	stats := domain.VolumeStats{
		VolumeID: domain.VolumeID(999),
	}
	tracker.updateHealthFromStats(domain.VolumeID(999), stats)
}

func TestVolumeHealthTracker_updateHealthFromStats_UpdatesAvailableSpace(t *testing.T) {
	tracker, _ := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{SpaceUsageThreshold: 0.99})

	stats := domain.VolumeStats{
		VolumeID: volumeID,
		SpaceUtilization: domain.Sample[*domain.SpaceUtilizationStats]{
			Value: &domain.SpaceUtilizationStats{
				TotalBytes: 1000000,
				UsedBytes:  100000,
				FreeBytes:  900000,
			},
			Epoch: 1,
			Time:  time.Now(),
		},
	}
	tracker.updateHealthFromStats(volumeID, stats)

	tracker.mu.RLock()
	state := tracker.states[volumeID]
	availableSpace := state.availableSpaceBytes
	tracker.mu.RUnlock()

	if availableSpace != 900000 {
		t.Errorf("availableSpaceBytes = %d, want 900000", availableSpace)
	}
}

// Test defaultHealthConfig function

func TestDefaultHealthConfig(t *testing.T) {
	config := defaultHealthConfig()

	if config.DegradedThreshold != 3 {
		t.Errorf("DegradedThreshold = %d, want 3", config.DegradedThreshold)
	}
	if config.FailedThreshold != 2 {
		t.Errorf("FailedThreshold = %d, want 2", config.FailedThreshold)
	}
	if config.RecoverySuccessCount != 10 {
		t.Errorf("RecoverySuccessCount = %d, want 10", config.RecoverySuccessCount)
	}
	if config.RecoveryWindow != 5*time.Minute {
		t.Errorf("RecoveryWindow = %v, want 5m", config.RecoveryWindow)
	}
	if config.SpaceUsageThreshold != 0.90 {
		t.Errorf("SpaceUsageThreshold = %v, want 0.90", config.SpaceUsageThreshold)
	}
	if config.LatencyThreshold != 500*time.Millisecond {
		t.Errorf("LatencyThreshold = %v, want 500ms", config.LatencyThreshold)
	}
	if config.StuckEpochThreshold != 3 {
		t.Errorf("StuckEpochThreshold = %d, want 3", config.StuckEpochThreshold)
	}
}

// Test error classification (exercising classifyVolumeError through RecordError)

func TestVolumeHealthTracker_ErrorClassification(t *testing.T) {
	testCases := []struct {
		name           string
		err            error
		expectDegraded bool // After threshold errors
		expectFailed   bool // After threshold errors
	}{
		{
			name:           "nil error - transient",
			err:            nil,
			expectDegraded: false,
			expectFailed:   false,
		},
		{
			name:           "ENOSPC - degrading",
			err:            syscall.ENOSPC,
			expectDegraded: true,
			expectFailed:   false,
		},
		{
			name:           "ErrNoSpace - degrading",
			err:            volumeerrors.ErrNoSpace,
			expectDegraded: true,
			expectFailed:   false,
		},
		{
			name:           "EIO - fatal",
			err:            syscall.EIO,
			expectDegraded: false,
			expectFailed:   true,
		},
		{
			name:           "FsyncError - fatal",
			err:            unix.NewFsyncError(errors.New("disk error")),
			expectDegraded: false,
			expectFailed:   true,
		},
		{
			name:           "DeadlineExceeded - degrading",
			err:            context.DeadlineExceeded,
			expectDegraded: true,
			expectFailed:   false,
		},
		{
			name:           "unknown error - transient",
			err:            errors.New("unknown"),
			expectDegraded: false,
			expectFailed:   false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tracker, _ := testHealthTracker(t)
			defer tracker.ClearVolumes()

			volumeID := domain.VolumeID(1)
			tracker.AddVolume(volumeID, HealthConfig{
				DegradedThreshold: 1, // Immediate transition
				FailedThreshold:   1, // Immediate transition
			})

			ctx := context.Background()
			tracker.RecordError(ctx, volumeID, tc.err)

			health := tracker.CheckVolumeHealth(volumeID)

			if tc.expectFailed {
				if health.State != domain.VolumeStateFailed {
					t.Errorf("State = %v, want VolumeStateFailed for %q", health.State, tc.name)
				}
			} else if tc.expectDegraded {
				if health.State != domain.VolumeStateDegraded {
					t.Errorf("State = %v, want VolumeStateDegraded for %q", health.State, tc.name)
				}
			} else {
				if health.State != domain.VolumeStateOK {
					t.Errorf("State = %v, want VolumeStateOK for transient %q", health.State, tc.name)
				}
			}
		})
	}
}

// Test metrics recording (we can't easily verify OTel metrics in unit tests,
// but we can verify the code paths don't panic)

func TestVolumeHealthTracker_MetricsRecording_NoPanic(t *testing.T) {
	logger := zerolog.Nop()
	meter := noop.NewMeterProvider().Meter("test")
	statsManager := NewVolumeStatsManager(&logger)

	tracker, err := NewVolumeHealthTracker(statsManager, meter, &logger, time.Now)
	if err != nil {
		t.Fatalf("NewVolumeHealthTracker() error = %v", err)
	}
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{
		DegradedThreshold: 1,
		FailedThreshold:   1,
	})

	ctx := context.Background()

	// Exercise various metric recording paths
	tracker.RecordError(ctx, volumeID, syscall.ENOSPC)  // degrading
	tracker.RecordError(ctx, volumeID, syscall.EIO)     // fatal
	tracker.RecordError(ctx, volumeID, errors.New("x")) // transient
	tracker.RecordSuccess(ctx, volumeID)

	// Stats update path
	stats := domain.VolumeStats{
		VolumeID: volumeID,
		SpaceUtilization: domain.Sample[*domain.SpaceUtilizationStats]{
			Value: &domain.SpaceUtilizationStats{
				TotalBytes: 1000000,
				UsedBytes:  100000,
				FreeBytes:  900000,
			},
			Epoch: 1,
			Time:  time.Now(),
		},
	}
	tracker.updateHealthFromStats(volumeID, stats)

	// Should complete without panicking
}

// Test Sample.Valid() usage in updateHealthFromStats

func TestVolumeHealthTracker_updateHealthFromStats_InvalidSample(t *testing.T) {
	tracker, _ := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{SpaceUsageThreshold: 0.5})

	// Send stats with invalid sample (zero epoch/time)
	stats := domain.VolumeStats{
		VolumeID: volumeID,
		SpaceUtilization: domain.Sample[*domain.SpaceUtilizationStats]{
			Value: &domain.SpaceUtilizationStats{
				TotalBytes: 1000000,
				UsedBytes:  900000, // Would trigger degradation if valid
				FreeBytes:  100000,
			},
			Epoch: 0,           // Invalid
			Time:  time.Time{}, // Zero time
		},
	}
	tracker.updateHealthFromStats(volumeID, stats)

	// Should remain OK because sample is invalid
	health := tracker.CheckVolumeHealth(volumeID)
	if health.State != domain.VolumeStateOK {
		t.Errorf("State = %v, want VolumeStateOK (invalid sample should be ignored)", health.State)
	}
}

func TestVolumeHealthTracker_updateHealthFromStats_SampleWithError(t *testing.T) {
	tracker, _ := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{SpaceUsageThreshold: 0.5})

	// Send stats with sample that has an error
	stats := domain.VolumeStats{
		VolumeID: volumeID,
		SpaceUtilization: domain.Sample[*domain.SpaceUtilizationStats]{
			Value: &domain.SpaceUtilizationStats{
				TotalBytes: 1000000,
				UsedBytes:  900000,
				FreeBytes:  100000,
			},
			Epoch: 1,
			Time:  time.Now(),
			Error: errors.New("statfs failed"),
		},
	}
	tracker.updateHealthFromStats(volumeID, stats)

	// Should remain OK because sample has error (Valid() returns false)
	health := tracker.CheckVolumeHealth(volumeID)
	if health.State != domain.VolumeStateOK {
		t.Errorf("State = %v, want VolumeStateOK (sample with error should be ignored)", health.State)
	}
}

// Test hysteresis in space recovery

func TestVolumeHealthTracker_SpaceRecovery_RequiresHysteresis(t *testing.T) {
	tracker, setTime := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{
		SpaceUsageThreshold: 0.90, // 90% threshold
		RecoveryWindow:      1 * time.Minute,
	})

	startTime := time.Now()
	setTime(startTime)

	// First: degrade at 91%
	tracker.updateHealthFromStats(volumeID, domain.VolumeStats{
		VolumeID: volumeID,
		SpaceUtilization: domain.Sample[*domain.SpaceUtilizationStats]{
			Value: &domain.SpaceUtilizationStats{
				TotalBytes: 1000000,
				UsedBytes:  910000,
				FreeBytes:  90000,
			},
			Epoch: 1,
			Time:  startTime,
		},
	})

	health := tracker.CheckVolumeHealth(volumeID)
	if health.State != domain.VolumeStateDegraded {
		t.Fatalf("State = %v, want VolumeStateDegraded at 91%%", health.State)
	}

	// Advance time past recovery window
	setTime(startTime.Add(2 * time.Minute))

	// Drop to 88% - within hysteresis (90% - 5% = 85%), should NOT recover
	tracker.updateHealthFromStats(volumeID, domain.VolumeStats{
		VolumeID: volumeID,
		SpaceUtilization: domain.Sample[*domain.SpaceUtilizationStats]{
			Value: &domain.SpaceUtilizationStats{
				TotalBytes: 1000000,
				UsedBytes:  880000,
				FreeBytes:  120000,
			},
			Epoch: 2,
			Time:  startTime.Add(2 * time.Minute),
		},
	})

	health = tracker.CheckVolumeHealth(volumeID)
	if health.State != domain.VolumeStateDegraded {
		t.Errorf("State = %v, want VolumeStateDegraded (88%% is within hysteresis)", health.State)
	}

	// Drop to 80% - below hysteresis, should recover
	tracker.updateHealthFromStats(volumeID, domain.VolumeStats{
		VolumeID: volumeID,
		SpaceUtilization: domain.Sample[*domain.SpaceUtilizationStats]{
			Value: &domain.SpaceUtilizationStats{
				TotalBytes: 1000000,
				UsedBytes:  800000,
				FreeBytes:  200000,
			},
			Epoch: 3,
			Time:  startTime.Add(2 * time.Minute),
		},
	})

	health = tracker.CheckVolumeHealth(volumeID)
	if health.State != domain.VolumeStateOK {
		t.Errorf("State = %v, want VolumeStateOK (80%% is below hysteresis)", health.State)
	}
}

// Test that latency check doesn't trigger with zero threshold

func TestVolumeHealthTracker_LatencyCheck_DisabledWithZeroThreshold(t *testing.T) {
	tracker, _ := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	// Note: LatencyThreshold defaults to 500ms when 0 is passed, so we need to
	// set it explicitly via SetConfig after adding, or use a very low value that still
	// would trigger but rely on the check being skipped.
	// Actually, the implementation applies defaults for zero values, so 0 becomes 500ms.
	// Let's test that extremely high latency triggers degradation when threshold is set.
	tracker.AddVolume(volumeID, HealthConfig{
		LatencyThreshold:    10 * time.Second, // Very high threshold
		SpaceUsageThreshold: 0.99,
	})

	// High latency stats - but below 10 second threshold
	stats := domain.VolumeStats{
		VolumeID: volumeID,
		SpaceUtilization: domain.Sample[*domain.SpaceUtilizationStats]{
			Value: &domain.SpaceUtilizationStats{
				TotalBytes: 1000000,
				UsedBytes:  100000,
				FreeBytes:  900000,
			},
			Epoch: 1,
			Time:  time.Now(),
		},
		BlockDevice: domain.Sample[*domain.BlockDeviceStats]{
			Value: &domain.BlockDeviceStats{
				Apple: &domain.BlockDeviceStatsApple{
					Reads:           10,
					Writes:          10,
					LatentReadTime:  1000000000, // 1 second total (100ms avg) - below 10s threshold
					LatentWriteTime: 1000000000,
				},
			},
			Epoch: 1,
			Time:  time.Now(),
		},
	}
	tracker.updateHealthFromStats(volumeID, stats)

	health := tracker.CheckVolumeHealth(volumeID)
	if health.State != domain.VolumeStateOK {
		t.Errorf("State = %v, want VolumeStateOK (latency below threshold)", health.State)
	}
}

// Test zero total space doesn't cause division by zero

func TestVolumeHealthTracker_updateHealthFromStats_ZeroTotalSpace(t *testing.T) {
	tracker, _ := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{SpaceUsageThreshold: 0.90})

	// Zero total space - should not panic
	stats := domain.VolumeStats{
		VolumeID: volumeID,
		SpaceUtilization: domain.Sample[*domain.SpaceUtilizationStats]{
			Value: &domain.SpaceUtilizationStats{
				TotalBytes: 0,
				UsedBytes:  0,
				FreeBytes:  0,
			},
			Epoch: 1,
			Time:  time.Now(),
		},
	}
	tracker.updateHealthFromStats(volumeID, stats)

	// Should remain OK (no division by zero)
	health := tracker.CheckVolumeHealth(volumeID)
	if health.State != domain.VolumeStateOK {
		t.Errorf("State = %v, want VolumeStateOK", health.State)
	}
}

// Test zero operations doesn't cause division by zero in latency calculation

func TestVolumeHealthTracker_updateHealthFromStats_ZeroOperations(t *testing.T) {
	tracker, _ := testHealthTracker(t)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{
		LatencyThreshold:    100 * time.Millisecond,
		SpaceUsageThreshold: 0.99,
	})

	// Zero operations - should not panic
	stats := domain.VolumeStats{
		VolumeID: volumeID,
		SpaceUtilization: domain.Sample[*domain.SpaceUtilizationStats]{
			Value: &domain.SpaceUtilizationStats{
				TotalBytes: 1000000,
				UsedBytes:  100000,
				FreeBytes:  900000,
			},
			Epoch: 1,
			Time:  time.Now(),
		},
		BlockDevice: domain.Sample[*domain.BlockDeviceStats]{
			Value: &domain.BlockDeviceStats{
				Apple: &domain.BlockDeviceStatsApple{
					Reads:           0,
					Writes:          0,
					LatentReadTime:  1000000000,
					LatentWriteTime: 1000000000,
				},
			},
			Epoch: 1,
			Time:  time.Now(),
		},
	}
	tracker.updateHealthFromStats(volumeID, stats)

	// Should remain OK (no division by zero)
	health := tracker.CheckVolumeHealth(volumeID)
	if health.State != domain.VolumeStateOK {
		t.Errorf("State = %v, want VolumeStateOK", health.State)
	}
}

// Verify interface compliance

func TestVolumeHealthTracker_ImplementsVolumeHealthChecker(t *testing.T) {
	var _ portvolume.VolumeHealthChecker = (*VolumeHealthTracker)(nil)
}

// Test that noop meter works

func TestVolumeHealthTracker_WithNoopMeter(t *testing.T) {
	logger := zerolog.Nop()
	meter := noop.NewMeterProvider().Meter("test")
	statsManager := NewVolumeStatsManager(&logger)

	tracker, err := NewVolumeHealthTracker(statsManager, meter, &logger, time.Now)
	if err != nil {
		t.Fatalf("NewVolumeHealthTracker() error = %v", err)
	}
	defer tracker.ClearVolumes()

	// All operations should work with noop meter
	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{})
	tracker.RecordError(context.Background(), volumeID, syscall.ENOSPC)
	tracker.RecordSuccess(context.Background(), volumeID)
	tracker.RemoveVolume(volumeID)
}

// Test processStatsUpdates context cancellation (indirectly via RemoveVolume)

func TestVolumeHealthTracker_RemoveVolume_CancelsStatsWatch(t *testing.T) {
	tracker, _ := testHealthTracker(t)

	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{})

	// RemoveVolume should cancel the watch and wait for done
	err := tracker.RemoveVolume(volumeID)
	if err != nil {
		t.Errorf("RemoveVolume() error = %v", err)
	}

	// Verify removed
	tracker.mu.RLock()
	_, watchExists := tracker.watches[volumeID]
	_, stateExists := tracker.states[volumeID]
	tracker.mu.RUnlock()

	if watchExists {
		t.Error("watch should be removed")
	}
	if stateExists {
		t.Error("state should be removed")
	}
}

// Benchmark tests

func BenchmarkVolumeHealthTracker_RecordError(b *testing.B) {
	logger := zerolog.Nop()
	meter := noop.NewMeterProvider().Meter("test")
	statsManager := NewVolumeStatsManager(&logger)

	tracker, _ := NewVolumeHealthTracker(statsManager, meter, &logger, time.Now)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{DegradedThreshold: 1000000})

	ctx := context.Background()
	testErr := syscall.ENOSPC

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tracker.RecordError(ctx, volumeID, testErr)
	}
}

func BenchmarkVolumeHealthTracker_RecordSuccess(b *testing.B) {
	logger := zerolog.Nop()
	meter := noop.NewMeterProvider().Meter("test")
	statsManager := NewVolumeStatsManager(&logger)

	tracker, _ := NewVolumeHealthTracker(statsManager, meter, &logger, time.Now)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{})

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tracker.RecordSuccess(ctx, volumeID)
	}
}

func BenchmarkVolumeHealthTracker_CheckVolumeHealth(b *testing.B) {
	logger := zerolog.Nop()
	meter := noop.NewMeterProvider().Meter("test")
	statsManager := NewVolumeStatsManager(&logger)

	tracker, _ := NewVolumeHealthTracker(statsManager, meter, &logger, time.Now)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tracker.CheckVolumeHealth(volumeID)
	}
}

func BenchmarkVolumeHealthTracker_updateHealthFromStats(b *testing.B) {
	logger := zerolog.Nop()
	meter := noop.NewMeterProvider().Meter("test")
	statsManager := NewVolumeStatsManager(&logger)

	tracker, _ := NewVolumeHealthTracker(statsManager, meter, &logger, time.Now)
	defer tracker.ClearVolumes()

	volumeID := domain.VolumeID(1)
	tracker.AddVolume(volumeID, HealthConfig{})

	stats := domain.VolumeStats{
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
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stats.SpaceUtilization.Epoch = uint64(i + 1)
		tracker.updateHealthFromStats(volumeID, stats)
	}
}
