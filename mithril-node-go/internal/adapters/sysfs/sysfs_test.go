//go:build linux
// +build linux

package adapterssysfs

import (
	"errors"
	"os"
	"testing"
)

func TestResolveBlockDevice_RootPath(t *testing.T) {
	blockDevice, err := ResolveBlockDevice("/")
	if err != nil {
		// On some systems (containers, VMs with virtual filesystems), this may fail
		if errors.Is(err, ErrVirtualFilesystem) {
			t.Skipf("Root filesystem is virtual: %v", err)
		}
		t.Fatalf("ResolveBlockDevice(\"/\") failed: %v", err)
	}

	if blockDevice.SysfsPath == "" {
		t.Error("SysfsPath should not be empty")
	}

	if blockDevice.Major == 0 {
		t.Error("Major device number should not be 0 for real block device")
	}

	t.Logf("Block device for /: sysfs=%s major=%d minor=%d",
		blockDevice.SysfsPath, blockDevice.Major, blockDevice.Minor)
}

func TestResolveBlockDevice_InvalidPath(t *testing.T) {
	_, err := ResolveBlockDevice("/nonexistent/path/that/does/not/exist")
	if err == nil {
		t.Error("ResolveBlockDevice should fail with invalid path")
	}
}

func TestResolveBlockDevice_VirtualFilesystem(t *testing.T) {
	// /proc is always a virtual filesystem (procfs)
	_, err := ResolveBlockDevice("/proc")
	if err == nil {
		t.Error("ResolveBlockDevice(/proc) should return error for virtual filesystem")
	}

	if !errors.Is(err, ErrVirtualFilesystem) {
		t.Errorf("Expected ErrVirtualFilesystem, got: %v", err)
	}
}

func TestResolveBlockDevice_Tmpfs(t *testing.T) {
	// /dev/shm is typically tmpfs
	if _, err := os.Stat("/dev/shm"); os.IsNotExist(err) {
		t.Skip("/dev/shm does not exist")
	}

	_, err := ResolveBlockDevice("/dev/shm")
	if err == nil {
		t.Error("ResolveBlockDevice(/dev/shm) should return error for tmpfs")
	}

	if !errors.Is(err, ErrVirtualFilesystem) {
		t.Errorf("Expected ErrVirtualFilesystem, got: %v", err)
	}
}

func TestResolveBlockDevice_Sysfs(t *testing.T) {
	// /sys is always sysfs
	_, err := ResolveBlockDevice("/sys")
	if err == nil {
		t.Error("ResolveBlockDevice(/sys) should return error for virtual filesystem")
	}

	if !errors.Is(err, ErrVirtualFilesystem) {
		t.Errorf("Expected ErrVirtualFilesystem, got: %v", err)
	}
}

func TestBlockDevice_StatPath(t *testing.T) {
	blockDevice, err := ResolveBlockDevice("/")
	if err != nil {
		if errors.Is(err, ErrVirtualFilesystem) {
			t.Skipf("Root filesystem is virtual: %v", err)
		}
		t.Fatalf("ResolveBlockDevice(\"/\") failed: %v", err)
	}

	statPath := blockDevice.StatPath()
	if statPath == "" {
		t.Error("StatPath should not be empty")
	}

	// Verify stat file exists
	if _, err := os.Stat(statPath); err != nil {
		t.Errorf("Stat file should exist at %s: %v", statPath, err)
	}

	t.Logf("Stat path: %s", statPath)
}

func TestBlockDevice_ReadFile(t *testing.T) {
	blockDevice, err := ResolveBlockDevice("/")
	if err != nil {
		if errors.Is(err, ErrVirtualFilesystem) {
			t.Skipf("Root filesystem is virtual: %v", err)
		}
		t.Fatalf("ResolveBlockDevice(\"/\") failed: %v", err)
	}

	// queue/rotational should exist for all block devices
	rotational, err := blockDevice.ReadFile("queue/rotational")
	if err != nil {
		t.Fatalf("ReadFile(\"queue/rotational\") failed: %v", err)
	}

	if rotational != "0" && rotational != "1" {
		t.Errorf("Expected rotational to be \"0\" or \"1\", got %q", rotational)
	}

	t.Logf("Rotational: %s (0=SSD, 1=HDD)", rotational)
}

func TestBlockDevice_ReadFile_NonExistent(t *testing.T) {
	blockDevice, err := ResolveBlockDevice("/")
	if err != nil {
		if errors.Is(err, ErrVirtualFilesystem) {
			t.Skipf("Root filesystem is virtual: %v", err)
		}
		t.Fatalf("ResolveBlockDevice(\"/\") failed: %v", err)
	}

	_, err = blockDevice.ReadFile("nonexistent/file/path")
	if err == nil {
		t.Error("ReadFile should fail for non-existent file")
	}
}

func TestBlockDevice_ReadFile_TrimsWhitespace(t *testing.T) {
	blockDevice, err := ResolveBlockDevice("/")
	if err != nil {
		if errors.Is(err, ErrVirtualFilesystem) {
			t.Skipf("Root filesystem is virtual: %v", err)
		}
		t.Fatalf("ResolveBlockDevice(\"/\") failed: %v", err)
	}

	// Read a file and verify no trailing newline
	content, err := blockDevice.ReadFile("queue/rotational")
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	if len(content) > 0 {
		lastChar := content[len(content)-1]
		if lastChar == '\n' || lastChar == '\r' || lastChar == ' ' || lastChar == '\t' {
			t.Errorf("ReadFile should trim trailing whitespace, got %q", content)
		}
	}
}

// Benchmark tests

func BenchmarkResolveBlockDevice(b *testing.B) {
	// Verify it works first
	_, err := ResolveBlockDevice("/")
	if err != nil {
		if errors.Is(err, ErrVirtualFilesystem) {
			b.Skipf("Root filesystem is virtual: %v", err)
		}
		b.Fatalf("ResolveBlockDevice failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ResolveBlockDevice("/")
	}
}

func BenchmarkBlockDevice_ReadFile(b *testing.B) {
	blockDevice, err := ResolveBlockDevice("/")
	if err != nil {
		if errors.Is(err, ErrVirtualFilesystem) {
			b.Skipf("Root filesystem is virtual: %v", err)
		}
		b.Fatalf("ResolveBlockDevice failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = blockDevice.ReadFile("queue/rotational")
	}
}
