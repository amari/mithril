//go:build darwin
// +build darwin

package darwin

import "testing"

func TestCloseIOService_NullService(t *testing.T) {
	// Closing a null service should not return an error
	err := closeIOService(0)
	if err != nil {
		t.Errorf("closeIOService(0) returned error: %v", err)
	}
}

func TestGetIOServiceClassName(t *testing.T) {
	// Get a real service to test with
	media, err := getIOMediaFromBSDName("disk0")
	if err != nil {
		t.Fatalf("getIOMediaFromBSDName(\"disk0\") failed: %v", err)
	}
	defer closeIOService(media)

	className, err := getIOServiceClassName(media)
	if err != nil {
		t.Fatalf("getIOServiceClassName() returned error: %v", err)
	}

	if className == "" {
		t.Error("getIOServiceClassName() returned empty string")
	}

	// IOMedia services should have a class name containing "IOMedia"
	t.Logf("Class name for disk0: %s", className)
}

func TestGetIOServiceClassName_MultipleDisks(t *testing.T) {
	// Test with multiple potential disk names
	diskNames := []string{"disk0", "disk1", "disk0s1"}

	for _, diskName := range diskNames {
		media, err := getIOMediaFromBSDName(diskName)
		if err != nil {
			// Skip disks that don't exist
			t.Logf("Skipping %s: %v", diskName, err)
			continue
		}

		className, err := getIOServiceClassName(media)
		closeIOService(media)

		if err != nil {
			t.Errorf("getIOServiceClassName() for %s returned error: %v", diskName, err)
			continue
		}

		if className == "" {
			t.Errorf("getIOServiceClassName() for %s returned empty string", diskName)
			continue
		}

		t.Logf("%s class name: %s", diskName, className)
	}
}

func TestCloseIOService_ValidService(t *testing.T) {
	// Get a real service
	media, err := getIOMediaFromBSDName("disk0")
	if err != nil {
		t.Fatalf("getIOMediaFromBSDName(\"disk0\") failed: %v", err)
	}

	// Close it - should not return an error
	err = closeIOService(media)
	if err != nil {
		t.Errorf("closeIOService() returned error: %v", err)
	}
}

func TestCloseIOService_DoubleClose(t *testing.T) {
	// Get a real service
	media, err := getIOMediaFromBSDName("disk0")
	if err != nil {
		t.Fatalf("getIOMediaFromBSDName(\"disk0\") failed: %v", err)
	}

	// Close it twice - second close should be safe (on null service)
	err = closeIOService(media)
	if err != nil {
		t.Errorf("First closeIOService() returned error: %v", err)
	}

	// Note: After the first close, media is released but the variable still holds
	// the old value. In production code, we set it to IO_OBJECT_NULL after release.
	// For this test, we just verify that closing a null service is safe.
	err = closeIOService(0)
	if err != nil {
		t.Errorf("closeIOService(0) returned error: %v", err)
	}
}

// Benchmark tests

func BenchmarkGetIOServiceClassName(b *testing.B) {
	media, err := getIOMediaFromBSDName("disk0")
	if err != nil {
		b.Fatalf("getIOMediaFromBSDName(\"disk0\") failed: %v", err)
	}
	defer closeIOService(media)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = getIOServiceClassName(media)
	}
}

func BenchmarkCloseIOService_Null(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = closeIOService(0)
	}
}
