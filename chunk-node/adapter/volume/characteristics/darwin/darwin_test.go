//go:build darwin
// +build darwin

package darwin

import (
	"os"
	"testing"
)

// TestGetVolumeCharacteristicsForPath_RootFS tests that we can detect characteristics
// for the root filesystem. This is a smoke test that verifies the detection pipeline
// works end-to-end without erroring.
func TestGetVolumeCharacteristicsForPath_RootFS(t *testing.T) {
	characteristics, err := GetVolumeCharacteristicsForPath("/")
	if err != nil {
		t.Fatalf("GetVolumeCharacteristicsForPath(/): unexpected error: %v", err)
	}

	if characteristics == nil {
		t.Fatal("GetVolumeCharacteristicsForPath(/): returned nil characteristics")
	}

	// DeviceName should be set
	if characteristics.DeviceName == "" {
		t.Error("expected DeviceName to be set")
	}

	t.Logf("Root filesystem characteristics:")
	t.Logf("  DeviceName:   %s", characteristics.DeviceName)
	t.Logf("  Medium:       %s", characteristics.Medium)
	t.Logf("  Interconnect: %s", characteristics.Interconnect)
	t.Logf("  Protocol:     %s", characteristics.Protocol)
	t.Logf("  Vendor:       %s", characteristics.Vendor)
	t.Logf("  Model:        %s", characteristics.Model)
	t.Logf("  SerialNumber: %s", characteristics.SerialNumber)
}

// TestGetVolumeCharacteristicsForPath_TempDir tests detection for the temp directory,
// which may be on a different volume than root.
func TestGetVolumeCharacteristicsForPath_TempDir(t *testing.T) {
	tempDir := os.TempDir()

	characteristics, err := GetVolumeCharacteristicsForPath(tempDir)
	if err != nil {
		t.Fatalf("GetVolumeCharacteristicsForPath(%s): unexpected error: %v", tempDir, err)
	}

	if characteristics == nil {
		t.Fatal("GetVolumeCharacteristicsForPath: returned nil characteristics")
	}

	if characteristics.DeviceName == "" {
		t.Error("expected DeviceName to be set")
	}

	t.Logf("Temp directory (%s) characteristics:", tempDir)
	t.Logf("  DeviceName:   %s", characteristics.DeviceName)
	t.Logf("  Medium:       %s", characteristics.Medium)
	t.Logf("  Interconnect: %s", characteristics.Interconnect)
	t.Logf("  Protocol:     %s", characteristics.Protocol)
}

// TestGetVolumeCharacteristicsForPath_HomeDir tests detection for the home directory.
func TestGetVolumeCharacteristicsForPath_HomeDir(t *testing.T) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		t.Skipf("could not determine home directory: %v", err)
	}

	characteristics, err := GetVolumeCharacteristicsForPath(homeDir)
	if err != nil {
		t.Fatalf("GetVolumeCharacteristicsForPath(%s): unexpected error: %v", homeDir, err)
	}

	if characteristics == nil {
		t.Fatal("GetVolumeCharacteristicsForPath: returned nil characteristics")
	}

	if characteristics.DeviceName == "" {
		t.Error("expected DeviceName to be set")
	}

	t.Logf("Home directory (%s) characteristics:", homeDir)
	t.Logf("  DeviceName:   %s", characteristics.DeviceName)
	t.Logf("  Medium:       %s", characteristics.Medium)
	t.Logf("  Interconnect: %s", characteristics.Interconnect)
	t.Logf("  Protocol:     %s", characteristics.Protocol)
}

// TestGetVolumeCharacteristicsForPath_NonExistent tests that a non-existent path
// returns an error.
func TestGetVolumeCharacteristicsForPath_NonExistent(t *testing.T) {
	_, err := GetVolumeCharacteristicsForPath("/this/path/does/not/exist/at/all")
	if err == nil {
		t.Error("expected error for non-existent path, got nil")
	}
}

// TestGetVolumeCharacteristicsForPath_File tests detection for a regular file,
// which should work the same as its containing directory.
func TestGetVolumeCharacteristicsForPath_File(t *testing.T) {
	// Create a temporary file
	f, err := os.CreateTemp("", "volume-characteristics-test-*")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(f.Name())
	defer f.Close()

	characteristics, err := GetVolumeCharacteristicsForPath(f.Name())
	if err != nil {
		t.Fatalf("GetVolumeCharacteristicsForPath(%s): unexpected error: %v", f.Name(), err)
	}

	if characteristics == nil {
		t.Fatal("GetVolumeCharacteristicsForPath: returned nil characteristics")
	}

	if characteristics.DeviceName == "" {
		t.Error("expected DeviceName to be set")
	}

	t.Logf("Temp file (%s) characteristics:", f.Name())
	t.Logf("  DeviceName: %s", characteristics.DeviceName)
}
