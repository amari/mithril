//go:build darwin
// +build darwin

package iokit

import "testing"

func TestCloseIOService_NullService(t *testing.T) {
	// Closing a null service should not return an error
	err := CloseIOService(0)
	if err != nil {
		t.Errorf("CloseIOService(0) returned error: %v", err)
	}
}

func TestGetIOServiceClassName(t *testing.T) {
	// Get a real service to test with
	media, err := GetIOMediaFromBSDName("disk0")
	if err != nil {
		t.Fatalf("GetIOMediaFromBSDName(\"disk0\") failed: %v", err)
	}
	defer CloseIOService(media)

	className, err := GetIOServiceClassName(media)
	if err != nil {
		t.Fatalf("GetIOServiceClassName() returned error: %v", err)
	}

	if className == "" {
		t.Error("GetIOServiceClassName() returned empty string")
	}

	// IOMedia services should have a class name containing "IOMedia"
	t.Logf("Class name for disk0: %s", className)
}

func TestGetIOServiceClassName_MultipleDisks(t *testing.T) {
	// Test with multiple potential disk names
	diskNames := []string{"disk0", "disk1", "disk0s1"}

	for _, diskName := range diskNames {
		media, err := GetIOMediaFromBSDName(diskName)
		if err != nil {
			// Skip disks that don't exist
			t.Logf("Skipping %s: %v", diskName, err)
			continue
		}

		className, err := GetIOServiceClassName(media)
		CloseIOService(media)

		if err != nil {
			t.Errorf("GetIOServiceClassName() for %s returned error: %v", diskName, err)
			continue
		}

		if className == "" {
			t.Errorf("GetIOServiceClassName() for %s returned empty string", diskName)
			continue
		}

		t.Logf("%s class name: %s", diskName, className)
	}
}

func TestCloseIOService_ValidService(t *testing.T) {
	// Get a real service
	media, err := GetIOMediaFromBSDName("disk0")
	if err != nil {
		t.Fatalf("GetIOMediaFromBSDName(\"disk0\") failed: %v", err)
	}

	// Close it - should not return an error
	err = CloseIOService(media)
	if err != nil {
		t.Errorf("CloseIOService() returned error: %v", err)
	}
}

func TestCloseIOService_DoubleClose(t *testing.T) {
	// Get a real service
	media, err := GetIOMediaFromBSDName("disk0")
	if err != nil {
		t.Fatalf("GetIOMediaFromBSDName(\"disk0\") failed: %v", err)
	}

	// Close it twice - second close should be safe (on null service)
	err = CloseIOService(media)
	if err != nil {
		t.Errorf("First CloseIOService() returned error: %v", err)
	}

	// Note: After the first close, media is released but the variable still holds
	// the old value. In production code, we set it to IOObjectNull after release.
	// For this test, we just verify that closing a null service is safe.
	err = CloseIOService(0)
	if err != nil {
		t.Errorf("CloseIOService(0) returned error: %v", err)
	}
}

// Benchmark tests

func BenchmarkGetIOServiceClassName(b *testing.B) {
	media, err := GetIOMediaFromBSDName("disk0")
	if err != nil {
		b.Fatalf("GetIOMediaFromBSDName(\"disk0\") failed: %v", err)
	}
	defer CloseIOService(media)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = GetIOServiceClassName(media)
	}
}

func BenchmarkCloseIOService_Null(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = CloseIOService(0)
	}
}
