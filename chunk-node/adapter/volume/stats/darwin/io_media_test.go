//go:build darwin
// +build darwin

package darwin

import (
	"errors"
	"testing"
)

func TestGetIOMediaFromBSDName(t *testing.T) {
	// Test with a disk that should exist on all macOS systems
	media, err := getIOMediaFromBSDName("disk0")
	if err != nil {
		t.Errorf("getIOMediaFromBSDName(\"disk0\") failed: %v", err)
	}
	if media == 0 {
		t.Error("getIOMediaFromBSDName(\"disk0\") returned nil media")
	}

	// Test with a non-existent disk to catch failure cases
	media, err = getIOMediaFromBSDName("disk999999")
	if err == nil {
		t.Error("getIOMediaFromBSDName(\"disk999999\") should have failed but didn't")
	}
	if !errors.Is(err, ErrIOReturnNoDevice) {
		t.Errorf("getIOMediaFromBSDName(\"disk999999\") should return ErrIOReturnNoDevice, got: %v", err)
	}
	if media != 0 {
		t.Error("getIOMediaFromBSDName(\"disk999999\") should have returned 0 media")
	}
}

func TestIOMediaFromBSDName(t *testing.T) {
	// Test with a disk that should exist on all macOS systems
	media, err := IOMediaFromBSDName("disk0")
	if err != nil {
		t.Fatalf("IOMediaFromBSDName(\"disk0\") failed: %v", err)
	}
	defer media.Close()

	if media.BSDName() != "disk0" {
		t.Errorf("BSDName() = %q, want \"disk0\"", media.BSDName())
	}
}

func TestIOMediaFromBSDName_NonExistent(t *testing.T) {
	media, err := IOMediaFromBSDName("disk999999")
	if err == nil {
		t.Error("IOMediaFromBSDName(\"disk999999\") should have failed but didn't")
		if media != nil {
			media.Close()
		}
	}
	if !errors.Is(err, ErrIOReturnNoDevice) {
		t.Errorf("expected ErrIOReturnNoDevice, got: %v", err)
	}
}

func TestIOMediaFromPath(t *testing.T) {
	// Test with the root filesystem which should always exist
	media, err := IOMediaFromPath("/")
	if err != nil {
		t.Fatalf("IOMediaFromPath(\"/\") failed: %v", err)
	}
	defer media.Close()

	bsdName := media.BSDName()
	if bsdName == "" {
		t.Error("BSDName() returned empty string")
	}
	t.Logf("Root filesystem BSD name: %s", bsdName)
}

func TestIOMediaFromPath_NonExistent(t *testing.T) {
	_, err := IOMediaFromPath("/nonexistent/path/that/does/not/exist")
	if err == nil {
		t.Error("IOMediaFromPath with non-existent path should have failed")
	}
}

func TestIOMedia_GetBlockStorageDriver(t *testing.T) {
	media, err := IOMediaFromBSDName("disk0")
	if err != nil {
		t.Fatalf("IOMediaFromBSDName(\"disk0\") failed: %v", err)
	}
	defer media.Close()

	driver, err := media.GetBlockStorageDriver()
	if err != nil {
		t.Fatalf("GetBlockStorageDriver() failed: %v", err)
	}
	defer driver.Close()

	className := driver.ClassName()
	if className == "" {
		t.Error("ClassName() returned empty string")
	}
	t.Logf("Block storage driver class name: %s", className)
}

func TestIOMedia_Close(t *testing.T) {
	media, err := IOMediaFromBSDName("disk0")
	if err != nil {
		t.Fatalf("IOMediaFromBSDName(\"disk0\") failed: %v", err)
	}

	// Close should not return an error
	if err := media.Close(); err != nil {
		t.Errorf("Close() returned error: %v", err)
	}

	// Double close should be safe (returns nil)
	if err := media.Close(); err != nil {
		t.Errorf("Double Close() returned error: %v", err)
	}
}

func TestIOBlockStorageDriver_ReadRawStatistics(t *testing.T) {
	media, err := IOMediaFromBSDName("disk0")
	if err != nil {
		t.Fatalf("IOMediaFromBSDName(\"disk0\") failed: %v", err)
	}
	defer media.Close()

	driver, err := media.GetBlockStorageDriver()
	if err != nil {
		t.Fatalf("GetBlockStorageDriver() failed: %v", err)
	}
	defer driver.Close()

	stats, err := driver.ReadRawStatistics()
	if err != nil {
		t.Fatalf("ReadRawStatistics() failed: %v", err)
	}

	// Verify expected keys exist in the statistics
	expectedKeys := []string{
		"Bytes (Read)",
		"Bytes (Write)",
		"Operations (Read)",
		"Operations (Write)",
	}

	for _, key := range expectedKeys {
		if _, ok := stats[key]; !ok {
			t.Errorf("ReadRawStatistics() missing expected key: %q", key)
		}
	}

	// Log all statistics for debugging
	t.Logf("Raw statistics: %+v", stats)
}

func TestIOBlockStorageDriver_ReadRawStatistics_Values(t *testing.T) {
	media, err := IOMediaFromBSDName("disk0")
	if err != nil {
		t.Fatalf("IOMediaFromBSDName(\"disk0\") failed: %v", err)
	}
	defer media.Close()

	driver, err := media.GetBlockStorageDriver()
	if err != nil {
		t.Fatalf("GetBlockStorageDriver() failed: %v", err)
	}
	defer driver.Close()

	stats, err := driver.ReadRawStatistics()
	if err != nil {
		t.Fatalf("ReadRawStatistics() failed: %v", err)
	}

	// Verify that numeric values are int64 (from CFNumber conversion)
	if bytesRead, ok := stats["Bytes (Read)"]; ok {
		if _, isInt64 := bytesRead.(int64); !isInt64 {
			t.Errorf("Bytes (Read) is not int64, got %T", bytesRead)
		}
	}

	if bytesWritten, ok := stats["Bytes (Write)"]; ok {
		if _, isInt64 := bytesWritten.(int64); !isInt64 {
			t.Errorf("Bytes (Write) is not int64, got %T", bytesWritten)
		}
	}

	// Verify read/write counts are non-negative
	if reads, ok := stats["Operations (Read)"].(int64); ok {
		if reads < 0 {
			t.Errorf("Operations (Read) = %d, expected non-negative", reads)
		}
	}

	if writes, ok := stats["Operations (Write)"].(int64); ok {
		if writes < 0 {
			t.Errorf("Operations (Write) = %d, expected non-negative", writes)
		}
	}
}

func TestIOBlockStorageDriver_Close(t *testing.T) {
	media, err := IOMediaFromBSDName("disk0")
	if err != nil {
		t.Fatalf("IOMediaFromBSDName(\"disk0\") failed: %v", err)
	}
	defer media.Close()

	driver, err := media.GetBlockStorageDriver()
	if err != nil {
		t.Fatalf("GetBlockStorageDriver() failed: %v", err)
	}

	// Close should not return an error
	if err := driver.Close(); err != nil {
		t.Errorf("Close() returned error: %v", err)
	}
}

// Integration test that exercises the full path from IOMedia to statistics
func TestIOMedia_FullIntegration(t *testing.T) {
	// Start from path
	media, err := IOMediaFromPath("/")
	if err != nil {
		t.Fatalf("IOMediaFromPath(\"/\") failed: %v", err)
	}
	defer media.Close()

	t.Logf("BSD Name: %s", media.BSDName())

	// Get driver
	driver, err := media.GetBlockStorageDriver()
	if err != nil {
		t.Fatalf("GetBlockStorageDriver() failed: %v", err)
	}
	defer driver.Close()

	t.Logf("Driver Class: %s", driver.ClassName())

	// Get statistics
	stats, err := driver.ReadRawStatistics()
	if err != nil {
		t.Fatalf("ReadRawStatistics() failed: %v", err)
	}

	// Verify we got some statistics
	if len(stats) == 0 {
		t.Error("ReadRawStatistics() returned empty map")
	}

	t.Logf("Statistics count: %d", len(stats))

	// Log specific values
	if bytesRead, ok := stats["Bytes (Read)"].(int64); ok {
		t.Logf("Bytes Read: %d", bytesRead)
	}
	if bytesWritten, ok := stats["Bytes (Write)"].(int64); ok {
		t.Logf("Bytes Written: %d", bytesWritten)
	}
	if reads, ok := stats["Operations (Read)"].(int64); ok {
		t.Logf("Read Operations: %d", reads)
	}
	if writes, ok := stats["Operations (Write)"].(int64); ok {
		t.Logf("Write Operations: %d", writes)
	}
}

// Test that multiple calls to ReadRawStatistics work correctly
func TestIOBlockStorageDriver_ReadRawStatistics_Multiple(t *testing.T) {
	media, err := IOMediaFromBSDName("disk0")
	if err != nil {
		t.Fatalf("IOMediaFromBSDName(\"disk0\") failed: %v", err)
	}
	defer media.Close()

	driver, err := media.GetBlockStorageDriver()
	if err != nil {
		t.Fatalf("GetBlockStorageDriver() failed: %v", err)
	}
	defer driver.Close()

	// Call ReadRawStatistics multiple times
	for i := 0; i < 3; i++ {
		stats, err := driver.ReadRawStatistics()
		if err != nil {
			t.Fatalf("ReadRawStatistics() iteration %d failed: %v", i, err)
		}
		if len(stats) == 0 {
			t.Errorf("ReadRawStatistics() iteration %d returned empty map", i)
		}
	}
}

// Benchmark tests

func BenchmarkGetIOMediaFromBSDName(b *testing.B) {
	for i := 0; i < b.N; i++ {
		media, err := getIOMediaFromBSDName("disk0")
		if err != nil {
			b.Fatalf("getIOMediaFromBSDName failed: %v", err)
		}
		_ = media
	}
}

func BenchmarkIOMediaFromBSDName(b *testing.B) {
	for i := 0; i < b.N; i++ {
		media, err := IOMediaFromBSDName("disk0")
		if err != nil {
			b.Fatalf("IOMediaFromBSDName failed: %v", err)
		}
		media.Close()
	}
}

func BenchmarkReadRawStatistics(b *testing.B) {
	media, err := IOMediaFromBSDName("disk0")
	if err != nil {
		b.Fatalf("IOMediaFromBSDName failed: %v", err)
	}
	defer media.Close()

	driver, err := media.GetBlockStorageDriver()
	if err != nil {
		b.Fatalf("GetBlockStorageDriver failed: %v", err)
	}
	defer driver.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := driver.ReadRawStatistics()
		if err != nil {
			b.Fatalf("ReadRawStatistics failed: %v", err)
		}
	}
}

func BenchmarkFullPath(b *testing.B) {
	for i := 0; i < b.N; i++ {
		media, err := IOMediaFromPath("/")
		if err != nil {
			b.Fatalf("IOMediaFromPath failed: %v", err)
		}

		driver, err := media.GetBlockStorageDriver()
		if err != nil {
			media.Close()
			b.Fatalf("GetBlockStorageDriver failed: %v", err)
		}

		_, err = driver.ReadRawStatistics()
		if err != nil {
			driver.Close()
			media.Close()
			b.Fatalf("ReadRawStatistics failed: %v", err)
		}

		driver.Close()
		media.Close()
	}
}
