//go:build darwin
// +build darwin

package darwin

import "testing"

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
	if media != 0 {
		t.Error("getIOMediaFromBSDName(\"disk999999\") should have returned 0 media")
	}
}
