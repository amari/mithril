package domain

import (
	"errors"
	"testing"
	"time"
)

// Sample tests

func TestSample_IsZero_NilSample(t *testing.T) {
	var s *Sample[int]
	if !s.IsZero() {
		t.Error("nil Sample should be zero")
	}
}

func TestSample_IsZero_ZeroTime(t *testing.T) {
	s := &Sample[int]{
		Value: 42,
		Epoch: 1,
		Time:  time.Time{},
	}
	if !s.IsZero() {
		t.Error("Sample with zero time should be zero")
	}
}

func TestSample_IsZero_ZeroEpoch(t *testing.T) {
	s := &Sample[int]{
		Value: 42,
		Epoch: 0,
		Time:  time.Now(),
	}
	if !s.IsZero() {
		t.Error("Sample with zero epoch should be zero")
	}
}

func TestSample_IsZero_ValidSample(t *testing.T) {
	s := &Sample[int]{
		Value: 42,
		Epoch: 1,
		Time:  time.Now(),
	}
	if s.IsZero() {
		t.Error("Sample with non-zero time and epoch should not be zero")
	}
}

func TestSample_Valid_NilSample(t *testing.T) {
	var s *Sample[int]
	if s.Valid() {
		t.Error("nil Sample should not be valid")
	}
}

func TestSample_Valid_WithError(t *testing.T) {
	s := &Sample[int]{
		Value: 42,
		Epoch: 1,
		Time:  time.Now(),
		Error: errors.New("test error"),
	}
	if s.Valid() {
		t.Error("Sample with error should not be valid")
	}
}

func TestSample_Valid_ZeroTime(t *testing.T) {
	s := &Sample[int]{
		Value: 42,
		Epoch: 1,
		Time:  time.Time{},
	}
	if s.Valid() {
		t.Error("Sample with zero time should not be valid")
	}
}

func TestSample_Valid_ZeroEpoch(t *testing.T) {
	s := &Sample[int]{
		Value: 42,
		Epoch: 0,
		Time:  time.Now(),
	}
	if s.Valid() {
		t.Error("Sample with zero epoch should not be valid")
	}
}

func TestSample_Valid_AllConditionsMet(t *testing.T) {
	s := &Sample[int]{
		Value: 42,
		Epoch: 1,
		Time:  time.Now(),
		Error: nil,
	}
	if !s.Valid() {
		t.Error("Sample with all conditions met should be valid")
	}
}

func TestSample_Compare_BothNil(t *testing.T) {
	var s, other *Sample[int]
	if s.Compare(other) != 0 {
		t.Error("comparing two nil samples should return 0")
	}
}

func TestSample_Compare_SelfNil(t *testing.T) {
	var s *Sample[int]
	other := &Sample[int]{Epoch: 1, Time: time.Now()}
	if s.Compare(other) != -1 {
		t.Error("nil sample should be less than non-nil sample")
	}
}

func TestSample_Compare_OtherNil(t *testing.T) {
	s := &Sample[int]{Epoch: 1, Time: time.Now()}
	var other *Sample[int]
	if s.Compare(other) != 1 {
		t.Error("non-nil sample should be greater than nil sample")
	}
}

func TestSample_Compare_DifferentTimes(t *testing.T) {
	now := time.Now()
	earlier := &Sample[int]{Epoch: 1, Time: now.Add(-time.Hour)}
	later := &Sample[int]{Epoch: 1, Time: now}

	if earlier.Compare(later) != -1 {
		t.Error("earlier sample should be less than later sample")
	}
	if later.Compare(earlier) != 1 {
		t.Error("later sample should be greater than earlier sample")
	}
}

func TestSample_Compare_SameTimeDifferentEpochs(t *testing.T) {
	now := time.Now()
	lowerEpoch := &Sample[int]{Epoch: 1, Time: now}
	higherEpoch := &Sample[int]{Epoch: 2, Time: now}

	if lowerEpoch.Compare(higherEpoch) != -1 {
		t.Error("sample with lower epoch should be less")
	}
	if higherEpoch.Compare(lowerEpoch) != 1 {
		t.Error("sample with higher epoch should be greater")
	}
}

func TestSample_Compare_Equal(t *testing.T) {
	now := time.Now()
	s1 := &Sample[int]{Epoch: 5, Time: now}
	s2 := &Sample[int]{Epoch: 5, Time: now}

	if s1.Compare(s2) != 0 {
		t.Error("identical samples should compare as equal")
	}
}

func TestSample_After(t *testing.T) {
	now := time.Now()
	earlier := &Sample[int]{Epoch: 1, Time: now.Add(-time.Hour)}
	later := &Sample[int]{Epoch: 1, Time: now}

	if !later.After(earlier) {
		t.Error("later.After(earlier) should be true")
	}
	if earlier.After(later) {
		t.Error("earlier.After(later) should be false")
	}
	if earlier.After(earlier) {
		t.Error("sample.After(itself) should be false")
	}
}

func TestSample_Before(t *testing.T) {
	now := time.Now()
	earlier := &Sample[int]{Epoch: 1, Time: now.Add(-time.Hour)}
	later := &Sample[int]{Epoch: 1, Time: now}

	if !earlier.Before(later) {
		t.Error("earlier.Before(later) should be true")
	}
	if later.Before(earlier) {
		t.Error("later.Before(earlier) should be false")
	}
	if earlier.Before(earlier) {
		t.Error("sample.Before(itself) should be false")
	}
}

func TestSample_GenericTypes(t *testing.T) {
	// Test with pointer type
	ptrSample := &Sample[*SpaceUtilizationStats]{
		Value: &SpaceUtilizationStats{TotalBytes: 1000},
		Epoch: 1,
		Time:  time.Now(),
	}
	if !ptrSample.Valid() {
		t.Error("pointer type sample should be valid")
	}

	// Test with string type
	strSample := &Sample[string]{
		Value: "test",
		Epoch: 1,
		Time:  time.Now(),
	}
	if !strSample.Valid() {
		t.Error("string type sample should be valid")
	}
}

// VolumeStats tests

func TestVolumeStats_Fields(t *testing.T) {
	now := time.Now()
	stats := VolumeStats{
		VolumeID: VolumeID(42),
		SpaceUtilization: Sample[*SpaceUtilizationStats]{
			Value: &SpaceUtilizationStats{
				TotalBytes: 1000000,
				UsedBytes:  500000,
				FreeBytes:  500000,
			},
			Epoch: 1,
			Time:  now,
		},
		BlockDevice: Sample[*BlockDeviceStats]{
			Value: &BlockDeviceStats{},
			Epoch: 1,
			Time:  now,
		},
	}

	if stats.VolumeID != VolumeID(42) {
		t.Errorf("VolumeID = %v, want 42", stats.VolumeID)
	}
	if stats.SpaceUtilization.Value.TotalBytes != 1000000 {
		t.Errorf("TotalBytes = %v, want 1000000", stats.SpaceUtilization.Value.TotalBytes)
	}
}

func TestVolumeStats_ZeroValue(t *testing.T) {
	var stats VolumeStats

	if stats.VolumeID != 0 {
		t.Errorf("zero VolumeStats.VolumeID = %v, want 0", stats.VolumeID)
	}
	if stats.SpaceUtilization.Valid() {
		t.Error("zero VolumeStats.SpaceUtilization should not be valid")
	}
	if stats.BlockDevice.Valid() {
		t.Error("zero VolumeStats.BlockDevice should not be valid")
	}
}

// SpaceUtilizationStats tests

func TestSpaceUtilizationStats_Fields(t *testing.T) {
	stats := SpaceUtilizationStats{
		TotalBytes: 1000000,
		UsedBytes:  400000,
		FreeBytes:  600000,
	}

	if stats.TotalBytes != 1000000 {
		t.Errorf("TotalBytes = %v, want 1000000", stats.TotalBytes)
	}
	if stats.UsedBytes != 400000 {
		t.Errorf("UsedBytes = %v, want 400000", stats.UsedBytes)
	}
	if stats.FreeBytes != 600000 {
		t.Errorf("FreeBytes = %v, want 600000", stats.FreeBytes)
	}
}

func TestSpaceUtilizationStats_ZeroValue(t *testing.T) {
	var stats SpaceUtilizationStats

	if stats.TotalBytes != 0 {
		t.Errorf("zero TotalBytes = %v, want 0", stats.TotalBytes)
	}
	if stats.UsedBytes != 0 {
		t.Errorf("zero UsedBytes = %v, want 0", stats.UsedBytes)
	}
	if stats.FreeBytes != 0 {
		t.Errorf("zero FreeBytes = %v, want 0", stats.FreeBytes)
	}
}

func TestSpaceUtilizationStats_LargeValues(t *testing.T) {
	// Test with large values (e.g., petabyte scale)
	stats := SpaceUtilizationStats{
		TotalBytes: 1 << 50, // ~1 PB
		UsedBytes:  1 << 49,
		FreeBytes:  1 << 49,
	}

	if stats.TotalBytes != 1<<50 {
		t.Errorf("TotalBytes = %v, want %v", stats.TotalBytes, int64(1<<50))
	}
}

// BlockDeviceStats tests

func TestBlockDeviceStats_Fields(t *testing.T) {
	stats := BlockDeviceStats{
		Apple: &BlockDeviceStatsApple{BytesRead: 1000},
		Linux: &BlockDeviceStatsLinux{Reads: 100},
	}

	if stats.Apple == nil {
		t.Error("Apple should not be nil")
	}
	if stats.Linux == nil {
		t.Error("Linux should not be nil")
	}
	if stats.Apple.BytesRead != 1000 {
		t.Errorf("Apple.BytesRead = %v, want 1000", stats.Apple.BytesRead)
	}
	if stats.Linux.Reads != 100 {
		t.Errorf("Linux.Reads = %v, want 100", stats.Linux.Reads)
	}
}

func TestBlockDeviceStats_ZeroValue(t *testing.T) {
	var stats BlockDeviceStats

	if stats.Apple != nil {
		t.Error("zero BlockDeviceStats.Apple should be nil")
	}
	if stats.Linux != nil {
		t.Error("zero BlockDeviceStats.Linux should be nil")
	}
}

func TestBlockDeviceStats_AppleOnly(t *testing.T) {
	stats := BlockDeviceStats{
		Apple: &BlockDeviceStatsApple{
			BytesRead:    1000,
			BytesWritten: 2000,
		},
	}

	if stats.Apple == nil {
		t.Error("Apple should not be nil")
	}
	if stats.Linux != nil {
		t.Error("Linux should be nil for Apple-only stats")
	}
}

func TestBlockDeviceStats_LinuxOnly(t *testing.T) {
	stats := BlockDeviceStats{
		Linux: &BlockDeviceStatsLinux{
			Reads:  100,
			Writes: 200,
		},
	}

	if stats.Linux == nil {
		t.Error("Linux should not be nil")
	}
	if stats.Apple != nil {
		t.Error("Apple should be nil for Linux-only stats")
	}
}

// BlockDeviceStatsApple tests

func TestBlockDeviceStatsApple_AllFields(t *testing.T) {
	stats := BlockDeviceStatsApple{
		BytesRead:       1000,
		BytesWritten:    2000,
		LatentReadTime:  100,
		LatentWriteTime: 200,
		ReadErrors:      1,
		ReadRetries:     2,
		Reads:           50,
		TotalReadTime:   500,
		TotalWriteTime:  600,
		WriteErrors:     3,
		WriteRetries:    4,
		Writes:          60,
	}

	if stats.BytesRead != 1000 {
		t.Errorf("BytesRead = %v, want 1000", stats.BytesRead)
	}
	if stats.BytesWritten != 2000 {
		t.Errorf("BytesWritten = %v, want 2000", stats.BytesWritten)
	}
	if stats.LatentReadTime != 100 {
		t.Errorf("LatentReadTime = %v, want 100", stats.LatentReadTime)
	}
	if stats.LatentWriteTime != 200 {
		t.Errorf("LatentWriteTime = %v, want 200", stats.LatentWriteTime)
	}
	if stats.ReadErrors != 1 {
		t.Errorf("ReadErrors = %v, want 1", stats.ReadErrors)
	}
	if stats.ReadRetries != 2 {
		t.Errorf("ReadRetries = %v, want 2", stats.ReadRetries)
	}
	if stats.Reads != 50 {
		t.Errorf("Reads = %v, want 50", stats.Reads)
	}
	if stats.TotalReadTime != 500 {
		t.Errorf("TotalReadTime = %v, want 500", stats.TotalReadTime)
	}
	if stats.TotalWriteTime != 600 {
		t.Errorf("TotalWriteTime = %v, want 600", stats.TotalWriteTime)
	}
	if stats.WriteErrors != 3 {
		t.Errorf("WriteErrors = %v, want 3", stats.WriteErrors)
	}
	if stats.WriteRetries != 4 {
		t.Errorf("WriteRetries = %v, want 4", stats.WriteRetries)
	}
	if stats.Writes != 60 {
		t.Errorf("Writes = %v, want 60", stats.Writes)
	}
}

func TestBlockDeviceStatsApple_ZeroValue(t *testing.T) {
	var stats BlockDeviceStatsApple

	if stats.BytesRead != 0 || stats.BytesWritten != 0 {
		t.Error("zero BlockDeviceStatsApple should have zero values")
	}
}

// BlockDeviceStatsLinux tests

func TestBlockDeviceStatsLinux_AllFields(t *testing.T) {
	stats := BlockDeviceStatsLinux{
		Reads:            100,
		MergedReads:      10,
		SectorsRead:      200,
		ReadTicks:        1000,
		Writes:           150,
		MergedWrites:     15,
		SectorsWritten:   300,
		WriteTicks:       1500,
		InFlight:         5,
		IOTicks:          2000,
		TimeInQueue:      2500,
		Discards:         20,
		MergedDiscards:   2,
		SectorsDiscarded: 40,
		DiscardTicks:     100,
		Flushes:          10,
		FlushTicks:       50,
	}

	if stats.Reads != 100 {
		t.Errorf("Reads = %v, want 100", stats.Reads)
	}
	if stats.MergedReads != 10 {
		t.Errorf("MergedReads = %v, want 10", stats.MergedReads)
	}
	if stats.SectorsRead != 200 {
		t.Errorf("SectorsRead = %v, want 200", stats.SectorsRead)
	}
	if stats.ReadTicks != 1000 {
		t.Errorf("ReadTicks = %v, want 1000", stats.ReadTicks)
	}
	if stats.Writes != 150 {
		t.Errorf("Writes = %v, want 150", stats.Writes)
	}
	if stats.MergedWrites != 15 {
		t.Errorf("MergedWrites = %v, want 15", stats.MergedWrites)
	}
	if stats.SectorsWritten != 300 {
		t.Errorf("SectorsWritten = %v, want 300", stats.SectorsWritten)
	}
	if stats.WriteTicks != 1500 {
		t.Errorf("WriteTicks = %v, want 1500", stats.WriteTicks)
	}
	if stats.InFlight != 5 {
		t.Errorf("InFlight = %v, want 5", stats.InFlight)
	}
	if stats.IOTicks != 2000 {
		t.Errorf("IOTicks = %v, want 2000", stats.IOTicks)
	}
	if stats.TimeInQueue != 2500 {
		t.Errorf("TimeInQueue = %v, want 2500", stats.TimeInQueue)
	}
	if stats.Discards != 20 {
		t.Errorf("Discards = %v, want 20", stats.Discards)
	}
	if stats.MergedDiscards != 2 {
		t.Errorf("MergedDiscards = %v, want 2", stats.MergedDiscards)
	}
	if stats.SectorsDiscarded != 40 {
		t.Errorf("SectorsDiscarded = %v, want 40", stats.SectorsDiscarded)
	}
	if stats.DiscardTicks != 100 {
		t.Errorf("DiscardTicks = %v, want 100", stats.DiscardTicks)
	}
	if stats.Flushes != 10 {
		t.Errorf("Flushes = %v, want 10", stats.Flushes)
	}
	if stats.FlushTicks != 50 {
		t.Errorf("FlushTicks = %v, want 50", stats.FlushTicks)
	}
}

func TestBlockDeviceStatsLinux_ZeroValue(t *testing.T) {
	var stats BlockDeviceStatsLinux

	if stats.Reads != 0 || stats.Writes != 0 || stats.InFlight != 0 {
		t.Error("zero BlockDeviceStatsLinux should have zero values")
	}
}

// Integration-style tests

func TestVolumeStats_WithValidSamples(t *testing.T) {
	now := time.Now()

	stats := VolumeStats{
		VolumeID: VolumeID(1),
		SpaceUtilization: Sample[*SpaceUtilizationStats]{
			Value: &SpaceUtilizationStats{
				TotalBytes: 1000000000,
				UsedBytes:  250000000,
				FreeBytes:  750000000,
			},
			Epoch: 10,
			Time:  now,
			Error: nil,
		},
		BlockDevice: Sample[*BlockDeviceStats]{
			Value: &BlockDeviceStats{
				Linux: &BlockDeviceStatsLinux{
					Reads:  1000,
					Writes: 500,
				},
			},
			Epoch: 10,
			Time:  now,
			Error: nil,
		},
	}

	if !stats.SpaceUtilization.Valid() {
		t.Error("SpaceUtilization should be valid")
	}
	if !stats.BlockDevice.Valid() {
		t.Error("BlockDevice should be valid")
	}
}

func TestVolumeStats_WithErrorSamples(t *testing.T) {
	now := time.Now()
	testErr := errors.New("disk read error")

	stats := VolumeStats{
		VolumeID: VolumeID(1),
		SpaceUtilization: Sample[*SpaceUtilizationStats]{
			Value: nil,
			Epoch: 10,
			Time:  now,
			Error: testErr,
		},
	}

	if stats.SpaceUtilization.Valid() {
		t.Error("SpaceUtilization with error should not be valid")
	}
	if stats.SpaceUtilization.Error != testErr {
		t.Errorf("Error = %v, want %v", stats.SpaceUtilization.Error, testErr)
	}
}

func TestSample_CompareWithTimePrecedence(t *testing.T) {
	// Time takes precedence over epoch
	baseTime := time.Now()

	s1 := &Sample[int]{Epoch: 100, Time: baseTime}
	s2 := &Sample[int]{Epoch: 1, Time: baseTime.Add(time.Nanosecond)}

	// s2 should be greater despite lower epoch because time is later
	if s1.Compare(s2) != -1 {
		t.Error("sample with earlier time should be less, regardless of epoch")
	}
	if !s2.After(s1) {
		t.Error("sample with later time should be after, regardless of epoch")
	}
}

func TestSample_EpochOnlyMattersWhenTimesEqual(t *testing.T) {
	now := time.Now()

	s1 := &Sample[int]{Epoch: 1, Time: now}
	s2 := &Sample[int]{Epoch: 2, Time: now}

	if s1.Compare(s2) != -1 {
		t.Error("when times equal, lower epoch should compare less")
	}
	if s2.Compare(s1) != 1 {
		t.Error("when times equal, higher epoch should compare greater")
	}
}
