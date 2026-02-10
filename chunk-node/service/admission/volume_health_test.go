package admission

import (
	"context"
	"errors"
	"testing"

	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/volumeerrors"
)

func TestAdmitReadWithVolumeHealth_NilHealth(t *testing.T) {
	err := AdmitReadWithVolumeHealth(context.Background(), nil)
	if err != nil {
		t.Errorf("AdmitReadWithVolumeHealth(nil) = %v, want nil", err)
	}
}

func TestAdmitReadWithVolumeHealth_StateUnknown(t *testing.T) {
	health := &domain.VolumeHealth{State: domain.VolumeStateUnknown}
	err := AdmitReadWithVolumeHealth(context.Background(), health)
	if err != nil {
		t.Errorf("AdmitReadWithVolumeHealth(Unknown) = %v, want nil", err)
	}
}

func TestAdmitReadWithVolumeHealth_StateOK(t *testing.T) {
	health := &domain.VolumeHealth{State: domain.VolumeStateOK}
	err := AdmitReadWithVolumeHealth(context.Background(), health)
	if err != nil {
		t.Errorf("AdmitReadWithVolumeHealth(OK) = %v, want nil", err)
	}
}

func TestAdmitReadWithVolumeHealth_StateDegraded(t *testing.T) {
	health := &domain.VolumeHealth{State: domain.VolumeStateDegraded}
	err := AdmitReadWithVolumeHealth(context.Background(), health)
	if err != nil {
		t.Errorf("AdmitReadWithVolumeHealth(Degraded) = %v, want nil (reads allowed when degraded)", err)
	}
}

func TestAdmitReadWithVolumeHealth_StateFailed(t *testing.T) {
	health := &domain.VolumeHealth{State: domain.VolumeStateFailed}
	err := AdmitReadWithVolumeHealth(context.Background(), health)

	if err == nil {
		t.Fatal("AdmitReadWithVolumeHealth(Failed) = nil, want error")
	}

	if !errors.Is(err, volumeerrors.ErrFailed) {
		t.Errorf("error should wrap ErrFailed, got %v", err)
	}

	var stateErr *volumeerrors.StateError
	if !errors.As(err, &stateErr) {
		t.Fatalf("error should be *volumeerrors.StateError, got %T", err)
	}

	if stateErr.State() != volumeerrors.StateFailed {
		t.Errorf("StateError.State() = %v, want StateFailed", stateErr.State())
	}
}

func TestAdmitWriteWithVolumeHealth_NilHealth(t *testing.T) {
	err := AdmitWriteWithVolumeHealth(context.Background(), nil)
	if err != nil {
		t.Errorf("AdmitWriteWithVolumeHealth(nil) = %v, want nil", err)
	}
}

func TestAdmitWriteWithVolumeHealth_StateUnknown(t *testing.T) {
	health := &domain.VolumeHealth{State: domain.VolumeStateUnknown}
	err := AdmitWriteWithVolumeHealth(context.Background(), health)
	if err != nil {
		t.Errorf("AdmitWriteWithVolumeHealth(Unknown) = %v, want nil", err)
	}
}

func TestAdmitWriteWithVolumeHealth_StateOK(t *testing.T) {
	health := &domain.VolumeHealth{State: domain.VolumeStateOK}
	err := AdmitWriteWithVolumeHealth(context.Background(), health)
	if err != nil {
		t.Errorf("AdmitWriteWithVolumeHealth(OK) = %v, want nil", err)
	}
}

func TestAdmitWriteWithVolumeHealth_StateDegraded(t *testing.T) {
	health := &domain.VolumeHealth{State: domain.VolumeStateDegraded}
	err := AdmitWriteWithVolumeHealth(context.Background(), health)

	if err == nil {
		t.Fatal("AdmitWriteWithVolumeHealth(Degraded) = nil, want error")
	}

	if !errors.Is(err, volumeerrors.ErrDegraded) {
		t.Errorf("error should wrap ErrDegraded, got %v", err)
	}

	var stateErr *volumeerrors.StateError
	if !errors.As(err, &stateErr) {
		t.Fatalf("error should be *volumeerrors.StateError, got %T", err)
	}

	if stateErr.State() != volumeerrors.StateDegraded {
		t.Errorf("StateError.State() = %v, want StateDegraded", stateErr.State())
	}
}

func TestAdmitWriteWithVolumeHealth_StateFailed(t *testing.T) {
	health := &domain.VolumeHealth{State: domain.VolumeStateFailed}
	err := AdmitWriteWithVolumeHealth(context.Background(), health)

	if err == nil {
		t.Fatal("AdmitWriteWithVolumeHealth(Failed) = nil, want error")
	}

	if !errors.Is(err, volumeerrors.ErrFailed) {
		t.Errorf("error should wrap ErrFailed, got %v", err)
	}

	var stateErr *volumeerrors.StateError
	if !errors.As(err, &stateErr) {
		t.Fatalf("error should be *volumeerrors.StateError, got %T", err)
	}

	if stateErr.State() != volumeerrors.StateFailed {
		t.Errorf("StateError.State() = %v, want StateFailed", stateErr.State())
	}
}

func TestAdmitReadWithVolumeHealth_AllStates(t *testing.T) {
	testCases := []struct {
		name        string
		state       domain.VolumeState
		expectError bool
		expectedErr error
	}{
		{"Unknown", domain.VolumeStateUnknown, false, nil},
		{"OK", domain.VolumeStateOK, false, nil},
		{"Degraded", domain.VolumeStateDegraded, false, nil},
		{"Failed", domain.VolumeStateFailed, true, volumeerrors.ErrFailed},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			health := &domain.VolumeHealth{State: tc.state}
			err := AdmitReadWithVolumeHealth(context.Background(), health)

			if tc.expectError {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if !errors.Is(err, tc.expectedErr) {
					t.Errorf("error = %v, want %v", err, tc.expectedErr)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestAdmitWriteWithVolumeHealth_AllStates(t *testing.T) {
	testCases := []struct {
		name        string
		state       domain.VolumeState
		expectError bool
		expectedErr error
	}{
		{"Unknown", domain.VolumeStateUnknown, false, nil},
		{"OK", domain.VolumeStateOK, false, nil},
		{"Degraded", domain.VolumeStateDegraded, true, volumeerrors.ErrDegraded},
		{"Failed", domain.VolumeStateFailed, true, volumeerrors.ErrFailed},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			health := &domain.VolumeHealth{State: tc.state}
			err := AdmitWriteWithVolumeHealth(context.Background(), health)

			if tc.expectError {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if !errors.Is(err, tc.expectedErr) {
					t.Errorf("error = %v, want %v", err, tc.expectedErr)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestAdmitReadWithVolumeHealth_ContextNotUsed(t *testing.T) {
	// Verify function works with various context states
	// Currently context is not used, but this tests the interface
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	health := &domain.VolumeHealth{State: domain.VolumeStateOK}
	err := AdmitReadWithVolumeHealth(ctx, health)

	// Function should still work even with cancelled context
	// (current implementation doesn't check context)
	if err != nil {
		t.Errorf("AdmitReadWithVolumeHealth with cancelled context = %v, want nil", err)
	}
}

func TestAdmitWriteWithVolumeHealth_ContextNotUsed(t *testing.T) {
	// Verify function works with various context states
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	health := &domain.VolumeHealth{State: domain.VolumeStateOK}
	err := AdmitWriteWithVolumeHealth(ctx, health)

	// Function should still work even with cancelled context
	if err != nil {
		t.Errorf("AdmitWriteWithVolumeHealth with cancelled context = %v, want nil", err)
	}
}

func TestReadVsWrite_DegradedStateDifference(t *testing.T) {
	// This test highlights the key difference between read and write admission:
	// Reads are allowed on degraded volumes, writes are not
	health := &domain.VolumeHealth{State: domain.VolumeStateDegraded}

	readErr := AdmitReadWithVolumeHealth(context.Background(), health)
	writeErr := AdmitWriteWithVolumeHealth(context.Background(), health)

	if readErr != nil {
		t.Errorf("reads should be allowed on degraded volumes, got error: %v", readErr)
	}

	if writeErr == nil {
		t.Error("writes should be rejected on degraded volumes, got nil error")
	}
}

func TestReadVsWrite_FailedStateBoth(t *testing.T) {
	// Both reads and writes should fail on failed volumes
	health := &domain.VolumeHealth{State: domain.VolumeStateFailed}

	readErr := AdmitReadWithVolumeHealth(context.Background(), health)
	writeErr := AdmitWriteWithVolumeHealth(context.Background(), health)

	if readErr == nil {
		t.Error("reads should be rejected on failed volumes")
	}
	if writeErr == nil {
		t.Error("writes should be rejected on failed volumes")
	}

	// Both should return ErrFailed
	if !errors.Is(readErr, volumeerrors.ErrFailed) {
		t.Errorf("read error should be ErrFailed, got %v", readErr)
	}
	if !errors.Is(writeErr, volumeerrors.ErrFailed) {
		t.Errorf("write error should be ErrFailed, got %v", writeErr)
	}
}

func TestStateError_ErrorMessage(t *testing.T) {
	health := &domain.VolumeHealth{State: domain.VolumeStateFailed}
	err := AdmitReadWithVolumeHealth(context.Background(), health)

	if err == nil {
		t.Fatal("expected error")
	}

	// Error message should match the underlying error
	if err.Error() != volumeerrors.ErrFailed.Error() {
		t.Errorf("Error() = %q, want %q", err.Error(), volumeerrors.ErrFailed.Error())
	}
}

func TestAdmitReadWithVolumeHealth_InvalidState(t *testing.T) {
	// Test with an invalid/undefined state value
	health := &domain.VolumeHealth{State: domain.VolumeState(999)}
	err := AdmitReadWithVolumeHealth(context.Background(), health)

	// Should not error - undefined states fall through to nil
	if err != nil {
		t.Errorf("AdmitReadWithVolumeHealth with invalid state = %v, want nil", err)
	}
}

func TestAdmitWriteWithVolumeHealth_InvalidState(t *testing.T) {
	// Test with an invalid/undefined state value
	health := &domain.VolumeHealth{State: domain.VolumeState(999)}
	err := AdmitWriteWithVolumeHealth(context.Background(), health)

	// Should not error - undefined states fall through to nil
	if err != nil {
		t.Errorf("AdmitWriteWithVolumeHealth with invalid state = %v, want nil", err)
	}
}
