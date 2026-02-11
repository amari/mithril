//go:build darwin
// +build darwin

package darwin

import (
	"testing"
)

func TestNewIOKitVolumeLabelCollector_RootPath(t *testing.T) {
	collector, err := NewIOKitVolumeLabelCollector("/")
	if err != nil {
		t.Fatalf("NewIOKitVolumeLabelCollector(\"/\") failed: %v", err)
	}
	defer collector.Close()
}

func TestNewIOKitVolumeLabelCollector_InvalidPath(t *testing.T) {
	_, err := NewIOKitVolumeLabelCollector("/nonexistent/path/that/does/not/exist")
	if err == nil {
		t.Error("NewIOKitVolumeLabelCollector should fail with invalid path")
	}
}

func TestIOKitVolumeLabelCollector_CollectVolumeLabels_RootPath(t *testing.T) {
	collector, err := NewIOKitVolumeLabelCollector("/")
	if err != nil {
		t.Fatalf("NewIOKitVolumeLabelCollector(\"/\") failed: %v", err)
	}
	defer collector.Close()

	labels, err := collector.CollectVolumeLabels()
	if err != nil {
		t.Fatalf("CollectVolumeLabels() returned error: %v", err)
	}

	// Check for known transport labels
	knownTransportLabels := []string{
		LabelNVMe,
		LabelUSB,
		LabelVirtIO,
		LabelAHCI,
		LabelPATA,
		LabelSAS,
		LabelSCSI,
	}

	transportDetected := ""
	for _, label := range knownTransportLabels {
		if val, ok := labels[label]; ok && val == "true" {
			transportDetected = label
			break
		}
	}

	if transportDetected != "" {
		t.Logf("Transport detected: %s=true", transportDetected)
	} else {
		t.Log("No transport detected (this may be expected for some configurations)")
	}

	t.Logf("All labels: %v", labels)
}

func TestIOKitVolumeLabelCollector_Close(t *testing.T) {
	collector, err := NewIOKitVolumeLabelCollector("/")
	if err != nil {
		t.Fatalf("NewIOKitVolumeLabelCollector(\"/\") failed: %v", err)
	}

	// Close should not return an error
	if err := collector.Close(); err != nil {
		t.Errorf("Close() returned error: %v", err)
	}

	// Double close should be safe
	if err := collector.Close(); err != nil {
		t.Errorf("Double Close() returned error: %v", err)
	}
}

func TestIOKitVolumeLabelCollector_CollectVolumeLabels_MultipleCallsSucceed(t *testing.T) {
	collector, err := NewIOKitVolumeLabelCollector("/")
	if err != nil {
		t.Fatalf("NewIOKitVolumeLabelCollector(\"/\") failed: %v", err)
	}
	defer collector.Close()

	// Multiple calls should succeed
	for i := 0; i < 3; i++ {
		_, err := collector.CollectVolumeLabels()
		if err != nil {
			t.Fatalf("CollectVolumeLabels() iteration %d returned error: %v", i, err)
		}
	}
}

func TestIOKitVolumeLabelCollector_DetectsTransport(t *testing.T) {
	collector, err := NewIOKitVolumeLabelCollector("/")
	if err != nil {
		t.Fatalf("NewIOKitVolumeLabelCollector(\"/\") failed: %v", err)
	}
	defer collector.Close()

	labels, err := collector.CollectVolumeLabels()
	if err != nil {
		t.Fatalf("CollectVolumeLabels() returned error: %v", err)
	}

	// Check which transport was detected
	switch {
	case labels[LabelNVMe] == "true":
		t.Log("NVMe storage detected (typical for modern Macs)")
	case labels[LabelAHCI] == "true":
		t.Log("AHCI/SATA storage detected")
	case labels[LabelVirtIO] == "true":
		t.Log("VirtIO storage detected (typical for VMs)")
	case labels[LabelUSB] == "true":
		t.Log("USB storage detected")
	case labels[LabelSCSI] == "true":
		t.Log("SCSI storage detected")
	case labels[LabelSAS] == "true":
		t.Log("SAS storage detected")
	case labels[LabelPATA] == "true":
		t.Log("PATA storage detected")
	default:
		t.Skip("No transport detected - skipping transport-specific assertions")
	}
}

func TestIOKitVolumeLabelCollector_LabelValueIsTrue(t *testing.T) {
	collector, err := NewIOKitVolumeLabelCollector("/")
	if err != nil {
		t.Fatalf("NewIOKitVolumeLabelCollector(\"/\") failed: %v", err)
	}
	defer collector.Close()

	labels, err := collector.CollectVolumeLabels()
	if err != nil {
		t.Fatalf("CollectVolumeLabels() returned error: %v", err)
	}

	// All transport labels should have value "true"
	transportLabels := []string{
		LabelNVMe,
		LabelUSB,
		LabelVirtIO,
		LabelAHCI,
		LabelPATA,
		LabelSAS,
		LabelSCSI,
	}

	for _, label := range transportLabels {
		if val, ok := labels[label]; ok {
			if val != "true" {
				t.Errorf("Label %s has value %q, expected \"true\"", label, val)
			}
		}
	}
}

// Benchmark tests

func BenchmarkNewIOKitVolumeLabelCollector(b *testing.B) {
	for i := 0; i < b.N; i++ {
		collector, err := NewIOKitVolumeLabelCollector("/")
		if err != nil {
			b.Fatalf("NewIOKitVolumeLabelCollector failed: %v", err)
		}
		collector.Close()
	}
}

func BenchmarkCollectVolumeLabels(b *testing.B) {
	collector, err := NewIOKitVolumeLabelCollector("/")
	if err != nil {
		b.Fatalf("NewIOKitVolumeLabelCollector failed: %v", err)
	}
	defer collector.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := collector.CollectVolumeLabels()
		if err != nil {
			b.Fatalf("CollectVolumeLabels failed: %v", err)
		}
	}
}
