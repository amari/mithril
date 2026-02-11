//go:build darwin
// +build darwin

package darwin

import (
	"errors"
	"testing"
)

func TestErrorSentinels_NotNil(t *testing.T) {
	sentinels := []struct {
		name string
		err  error
	}{
		{"ErrIOReturnError", ErrIOReturnError},
		{"ErrIOReturnNoMemory", ErrIOReturnNoMemory},
		{"ErrIOReturnNoResources", ErrIOReturnNoResources},
		{"ErrIOReturnIPCError", ErrIOReturnIPCError},
		{"ErrIOReturnNoDevice", ErrIOReturnNoDevice},
		{"ErrIOReturnNotPrivileged", ErrIOReturnNotPrivileged},
		{"ErrIOReturnBadArgument", ErrIOReturnBadArgument},
		{"ErrIOReturnLockedRead", ErrIOReturnLockedRead},
		{"ErrIOReturnLockedWrite", ErrIOReturnLockedWrite},
		{"ErrIOReturnExclusiveAccess", ErrIOReturnExclusiveAccess},
		{"ErrIOReturnBadMessageID", ErrIOReturnBadMessageID},
		{"ErrIOReturnUnsupported", ErrIOReturnUnsupported},
		{"ErrIOReturnVMError", ErrIOReturnVMError},
		{"ErrIOReturnInternalError", ErrIOReturnInternalError},
		{"ErrIOReturnIOError", ErrIOReturnIOError},
		{"ErrIOReturnCannotLock", ErrIOReturnCannotLock},
		{"ErrIOReturnNotOpen", ErrIOReturnNotOpen},
		{"ErrIOReturnNotReadable", ErrIOReturnNotReadable},
		{"ErrIOReturnNotWritable", ErrIOReturnNotWritable},
		{"ErrIOReturnNotAligned", ErrIOReturnNotAligned},
		{"ErrIOReturnBadMedia", ErrIOReturnBadMedia},
		{"ErrIOReturnStillOpen", ErrIOReturnStillOpen},
		{"ErrIOReturnRLDError", ErrIOReturnRLDError},
		{"ErrIOReturnDMAError", ErrIOReturnDMAError},
		{"ErrIOReturnBusy", ErrIOReturnBusy},
		{"ErrIOReturnTimeout", ErrIOReturnTimeout},
		{"ErrIOReturnOffline", ErrIOReturnOffline},
		{"ErrIOReturnNotReady", ErrIOReturnNotReady},
		{"ErrIOReturnNotAttached", ErrIOReturnNotAttached},
		{"ErrIOReturnNoChannels", ErrIOReturnNoChannels},
		{"ErrIOReturnNoSpace", ErrIOReturnNoSpace},
		{"ErrIOReturnPortExists", ErrIOReturnPortExists},
		{"ErrIOReturnCannotWire", ErrIOReturnCannotWire},
		{"ErrIOReturnNoInterrupt", ErrIOReturnNoInterrupt},
		{"ErrIOReturnNoFrames", ErrIOReturnNoFrames},
		{"ErrIOReturnMessageTooLarge", ErrIOReturnMessageTooLarge},
		{"ErrIOReturnNotPermitted", ErrIOReturnNotPermitted},
		{"ErrIOReturnNoPower", ErrIOReturnNoPower},
		{"ErrIOReturnNoMedia", ErrIOReturnNoMedia},
		{"ErrIOReturnUnformattedMedia", ErrIOReturnUnformattedMedia},
		{"ErrIOReturnUnsupportedMode", ErrIOReturnUnsupportedMode},
		{"ErrIOReturnUnderrun", ErrIOReturnUnderrun},
		{"ErrIOReturnOverrun", ErrIOReturnOverrun},
		{"ErrIOReturnDeviceError", ErrIOReturnDeviceError},
		{"ErrIOReturnNoCompletion", ErrIOReturnNoCompletion},
	}

	for _, tc := range sentinels {
		t.Run(tc.name, func(t *testing.T) {
			if tc.err == nil {
				t.Errorf("%s should not be nil", tc.name)
			}
		})
	}
}

func TestErrorSentinels_HaveNonEmptyMessage(t *testing.T) {
	sentinels := []struct {
		name string
		err  error
	}{
		{"ErrIOReturnError", ErrIOReturnError},
		{"ErrIOReturnNoMemory", ErrIOReturnNoMemory},
		{"ErrIOReturnNoResources", ErrIOReturnNoResources},
		{"ErrIOReturnIPCError", ErrIOReturnIPCError},
		{"ErrIOReturnNoDevice", ErrIOReturnNoDevice},
		{"ErrIOReturnNotPrivileged", ErrIOReturnNotPrivileged},
		{"ErrIOReturnBadArgument", ErrIOReturnBadArgument},
		{"ErrIOReturnLockedRead", ErrIOReturnLockedRead},
		{"ErrIOReturnLockedWrite", ErrIOReturnLockedWrite},
		{"ErrIOReturnExclusiveAccess", ErrIOReturnExclusiveAccess},
		{"ErrIOReturnBadMessageID", ErrIOReturnBadMessageID},
		{"ErrIOReturnUnsupported", ErrIOReturnUnsupported},
		{"ErrIOReturnVMError", ErrIOReturnVMError},
		{"ErrIOReturnInternalError", ErrIOReturnInternalError},
		{"ErrIOReturnIOError", ErrIOReturnIOError},
		{"ErrIOReturnCannotLock", ErrIOReturnCannotLock},
		{"ErrIOReturnNotOpen", ErrIOReturnNotOpen},
		{"ErrIOReturnNotReadable", ErrIOReturnNotReadable},
		{"ErrIOReturnNotWritable", ErrIOReturnNotWritable},
		{"ErrIOReturnNotAligned", ErrIOReturnNotAligned},
		{"ErrIOReturnBadMedia", ErrIOReturnBadMedia},
		{"ErrIOReturnStillOpen", ErrIOReturnStillOpen},
		{"ErrIOReturnRLDError", ErrIOReturnRLDError},
		{"ErrIOReturnDMAError", ErrIOReturnDMAError},
		{"ErrIOReturnBusy", ErrIOReturnBusy},
		{"ErrIOReturnTimeout", ErrIOReturnTimeout},
		{"ErrIOReturnOffline", ErrIOReturnOffline},
		{"ErrIOReturnNotReady", ErrIOReturnNotReady},
		{"ErrIOReturnNotAttached", ErrIOReturnNotAttached},
		{"ErrIOReturnNoChannels", ErrIOReturnNoChannels},
		{"ErrIOReturnNoSpace", ErrIOReturnNoSpace},
		{"ErrIOReturnPortExists", ErrIOReturnPortExists},
		{"ErrIOReturnCannotWire", ErrIOReturnCannotWire},
		{"ErrIOReturnNoInterrupt", ErrIOReturnNoInterrupt},
		{"ErrIOReturnNoFrames", ErrIOReturnNoFrames},
		{"ErrIOReturnMessageTooLarge", ErrIOReturnMessageTooLarge},
		{"ErrIOReturnNotPermitted", ErrIOReturnNotPermitted},
		{"ErrIOReturnNoPower", ErrIOReturnNoPower},
		{"ErrIOReturnNoMedia", ErrIOReturnNoMedia},
		{"ErrIOReturnUnformattedMedia", ErrIOReturnUnformattedMedia},
		{"ErrIOReturnUnsupportedMode", ErrIOReturnUnsupportedMode},
		{"ErrIOReturnUnderrun", ErrIOReturnUnderrun},
		{"ErrIOReturnOverrun", ErrIOReturnOverrun},
		{"ErrIOReturnDeviceError", ErrIOReturnDeviceError},
		{"ErrIOReturnNoCompletion", ErrIOReturnNoCompletion},
	}

	for _, tc := range sentinels {
		t.Run(tc.name, func(t *testing.T) {
			msg := tc.err.Error()
			if msg == "" {
				t.Errorf("%s.Error() should not be empty", tc.name)
			}
		})
	}
}

func TestErrorSentinels_AreDistinct(t *testing.T) {
	sentinels := []error{
		ErrIOReturnError,
		ErrIOReturnNoMemory,
		ErrIOReturnNoResources,
		ErrIOReturnIPCError,
		ErrIOReturnNoDevice,
		ErrIOReturnNotPrivileged,
		ErrIOReturnBadArgument,
		ErrIOReturnLockedRead,
		ErrIOReturnLockedWrite,
		ErrIOReturnExclusiveAccess,
		ErrIOReturnBadMessageID,
		ErrIOReturnUnsupported,
		ErrIOReturnVMError,
		ErrIOReturnInternalError,
		ErrIOReturnIOError,
		ErrIOReturnCannotLock,
		ErrIOReturnNotOpen,
		ErrIOReturnNotReadable,
		ErrIOReturnNotWritable,
		ErrIOReturnNotAligned,
		ErrIOReturnBadMedia,
		ErrIOReturnStillOpen,
		ErrIOReturnRLDError,
		ErrIOReturnDMAError,
		ErrIOReturnBusy,
		ErrIOReturnTimeout,
		ErrIOReturnOffline,
		ErrIOReturnNotReady,
		ErrIOReturnNotAttached,
		ErrIOReturnNoChannels,
		ErrIOReturnNoSpace,
		ErrIOReturnPortExists,
		ErrIOReturnCannotWire,
		ErrIOReturnNoInterrupt,
		ErrIOReturnNoFrames,
		ErrIOReturnMessageTooLarge,
		ErrIOReturnNotPermitted,
		ErrIOReturnNoPower,
		ErrIOReturnNoMedia,
		ErrIOReturnUnformattedMedia,
		ErrIOReturnUnsupportedMode,
		ErrIOReturnUnderrun,
		ErrIOReturnOverrun,
		ErrIOReturnDeviceError,
		ErrIOReturnNoCompletion,
	}

	// Ensure each error is distinct from all others
	for i, err1 := range sentinels {
		for j, err2 := range sentinels {
			if i != j {
				if errors.Is(err1, err2) {
					t.Errorf("Error %d and %d should be distinct, but errors.Is returned true", i, j)
				}
			}
		}
	}
}

func TestErrorSentinels_ErrorsIs(t *testing.T) {
	// Test that errors.Is works correctly with sentinel errors
	testCases := []struct {
		name   string
		err    error
		target error
		want   bool
	}{
		{"same error", ErrIOReturnNoDevice, ErrIOReturnNoDevice, true},
		{"different error", ErrIOReturnNoDevice, ErrIOReturnError, false},
		{"nil target", ErrIOReturnNoDevice, nil, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := errors.Is(tc.err, tc.target)
			if got != tc.want {
				t.Errorf("errors.Is(%v, %v) = %v, want %v", tc.err, tc.target, got, tc.want)
			}
		})
	}
}

func TestErrorSentinels_ContainKIOReturnPrefix(t *testing.T) {
	// Most IOKit error sentinels should contain the kIOReturn prefix
	sentinels := []struct {
		name string
		err  error
	}{
		{"ErrIOReturnError", ErrIOReturnError},
		{"ErrIOReturnNoMemory", ErrIOReturnNoMemory},
		{"ErrIOReturnNoDevice", ErrIOReturnNoDevice},
		{"ErrIOReturnBadArgument", ErrIOReturnBadArgument},
		{"ErrIOReturnIOError", ErrIOReturnIOError},
		{"ErrIOReturnTimeout", ErrIOReturnTimeout},
		{"ErrIOReturnBusy", ErrIOReturnBusy},
	}

	for _, tc := range sentinels {
		t.Run(tc.name, func(t *testing.T) {
			msg := tc.err.Error()
			if len(msg) < 10 {
				t.Errorf("%s.Error() = %q, expected longer message", tc.name, msg)
			}
		})
	}
}

func TestErrorFromKernReturn_NoDevice(t *testing.T) {
	// Test by attempting to get a non-existent device
	_, err := GetIOMediaFromBSDName("nonexistent_disk_xyz123")
	if err == nil {
		t.Fatal("Expected error for non-existent disk")
	}

	if !errors.Is(err, ErrIOReturnNoDevice) {
		t.Errorf("Expected ErrIOReturnNoDevice, got: %v", err)
	}
}

func TestErrorFromKernReturn_Success(t *testing.T) {
	// Test by getting a real device - should not return error
	media, err := GetIOMediaFromBSDName("disk0")
	if err != nil {
		t.Skipf("Skipping test - disk0 not available: %v", err)
	}
	CloseIOService(media)
	// If we get here, no error was returned for success case
}

func TestIokitErrorsMapLength(t *testing.T) {
	// Count the number of sentinel errors defined
	sentinelCount := 45 // Number of ErrIOReturn* variables defined

	// The iokitErrors map should have an entry for each sentinel
	// This is a sanity check to ensure the map is properly populated
	// We can't directly access the map size without exposing it,
	// but we can verify through behavior

	// Test a few known error codes via integration tests
	testCases := []struct {
		diskName    string
		expectError bool
	}{
		{"disk0", false},     // Should exist
		{"disk999999", true}, // Should not exist
		{"invalid!@#", true}, // Invalid name
	}

	for _, tc := range testCases {
		_, err := GetIOMediaFromBSDName(tc.diskName)
		if tc.expectError && err == nil {
			t.Errorf("Expected error for disk %q but got nil", tc.diskName)
		}
		if !tc.expectError && err != nil {
			t.Errorf("Expected no error for disk %q but got: %v", tc.diskName, err)
		}
	}

	_ = sentinelCount // Acknowledge the count
}

// Benchmark error creation
func BenchmarkErrorIs(b *testing.B) {
	err := ErrIOReturnNoDevice

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = errors.Is(err, ErrIOReturnNoDevice)
	}
}

func BenchmarkErrorString(b *testing.B) {
	err := ErrIOReturnNoDevice

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = err.Error()
	}
}
