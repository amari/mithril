//go:build darwin
// +build darwin

package darwin

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestNewIOKitBlockDeviceStatsCollector(t *testing.T) {
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()
	ctx := context.Background()

	collector, err := NewIOKitBlockDeviceStatsCollector(ctx, "/", 100*time.Millisecond, &logger, time.Now)
	if err != nil {
		t.Fatalf("NewIOKitBlockDeviceStatsCollector failed: %v", err)
	}
	defer collector.Close()
}

func TestNewIOKitBlockDeviceStatsCollector_InvalidPath(t *testing.T) {
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()
	ctx := context.Background()

	_, err := NewIOKitBlockDeviceStatsCollector(ctx, "/nonexistent/path/that/does/not/exist", 100*time.Millisecond, &logger, time.Now)
	if err == nil {
		t.Error("NewIOKitBlockDeviceStatsCollector should fail with invalid path")
	}
}

func TestIOKitBlockDeviceStatsCollector_CollectVolumeStats(t *testing.T) {
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()
	ctx := context.Background()

	fixedTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	nowFunc := func() time.Time { return fixedTime }

	collector, err := NewIOKitBlockDeviceStatsCollector(ctx, "/", 50*time.Millisecond, &logger, nowFunc)
	if err != nil {
		t.Fatalf("NewIOKitBlockDeviceStatsCollector failed: %v", err)
	}
	defer collector.Close()

	// Wait for at least one poll to complete
	time.Sleep(100 * time.Millisecond)

	sample, err := collector.CollectVolumeStats()
	if err != nil {
		t.Fatalf("CollectVolumeStats() returned error: %v", err)
	}

	// Verify sample has been populated
	if sample.Epoch == 0 {
		t.Error("sample.Epoch should be non-zero after polling")
	}

	if sample.Time.IsZero() {
		t.Error("sample.Time should be non-zero after polling")
	}

	if sample.Error != nil {
		t.Errorf("sample.Error should be nil, got: %v", sample.Error)
	}

	if sample.Value == nil {
		t.Fatal("sample.Value should not be nil")
	}

	if sample.Value.Apple == nil {
		t.Fatal("sample.Value.Apple should not be nil")
	}

	// Verify some statistics values (should be non-negative)
	appleStats := sample.Value.Apple
	if appleStats.BytesRead < 0 {
		t.Errorf("BytesRead = %d, expected non-negative", appleStats.BytesRead)
	}
	if appleStats.BytesWritten < 0 {
		t.Errorf("BytesWritten = %d, expected non-negative", appleStats.BytesWritten)
	}
	if appleStats.Reads < 0 {
		t.Errorf("Reads = %d, expected non-negative", appleStats.Reads)
	}
	if appleStats.Writes < 0 {
		t.Errorf("Writes = %d, expected non-negative", appleStats.Writes)
	}

	t.Logf("Sample: Epoch=%d, Time=%v", sample.Epoch, sample.Time)
	t.Logf("Apple Stats: BytesRead=%d, BytesWritten=%d, Reads=%d, Writes=%d",
		appleStats.BytesRead, appleStats.BytesWritten, appleStats.Reads, appleStats.Writes)
}

func TestIOKitBlockDeviceStatsCollector_NowFuncInjection(t *testing.T) {
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()
	ctx := context.Background()

	// Use a fixed time to verify injection works
	fixedTime := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	var callCount atomic.Int32
	nowFunc := func() time.Time {
		callCount.Add(1)
		return fixedTime
	}

	collector, err := NewIOKitBlockDeviceStatsCollector(ctx, "/", 50*time.Millisecond, &logger, nowFunc)
	if err != nil {
		t.Fatalf("NewIOKitBlockDeviceStatsCollector failed: %v", err)
	}
	defer collector.Close()

	// Wait for poll
	time.Sleep(100 * time.Millisecond)

	sample, err := collector.CollectVolumeStats()
	if err != nil {
		t.Fatalf("CollectVolumeStats() returned error: %v", err)
	}

	// Verify the injected time function was used
	if callCount.Load() == 0 {
		t.Error("nowFunc should have been called at least once")
	}

	// The sample time should reflect our fixed time
	if !sample.Time.Equal(fixedTime) {
		t.Errorf("sample.Time = %v, want %v", sample.Time, fixedTime)
	}
}

func TestIOKitBlockDeviceStatsCollector_EpochIncrementsOnPoll(t *testing.T) {
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()
	ctx := context.Background()

	pollInterval := 50 * time.Millisecond
	collector, err := NewIOKitBlockDeviceStatsCollector(ctx, "/", pollInterval, &logger, time.Now)
	if err != nil {
		t.Fatalf("NewIOKitBlockDeviceStatsCollector failed: %v", err)
	}
	defer collector.Close()

	// Wait for first poll
	time.Sleep(pollInterval + 20*time.Millisecond)

	sample1, err := collector.CollectVolumeStats()
	if err != nil {
		t.Fatalf("CollectVolumeStats() returned error: %v", err)
	}
	epoch1 := sample1.Epoch

	// Wait for another poll
	time.Sleep(pollInterval + 20*time.Millisecond)

	sample2, err := collector.CollectVolumeStats()
	if err != nil {
		t.Fatalf("CollectVolumeStats() returned error: %v", err)
	}
	epoch2 := sample2.Epoch

	if epoch2 <= epoch1 {
		t.Errorf("Epoch should increment: epoch1=%d, epoch2=%d", epoch1, epoch2)
	}

	t.Logf("Epoch progression: %d -> %d", epoch1, epoch2)
}

func TestIOKitBlockDeviceStatsCollector_Close(t *testing.T) {
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()
	ctx := context.Background()

	collector, err := NewIOKitBlockDeviceStatsCollector(ctx, "/", 100*time.Millisecond, &logger, time.Now)
	if err != nil {
		t.Fatalf("NewIOKitBlockDeviceStatsCollector failed: %v", err)
	}

	// Close should not return an error
	if err := collector.Close(); err != nil {
		t.Errorf("Close() returned error: %v", err)
	}
}

func TestIOKitBlockDeviceStatsCollector_CloseStopsPollLoop(t *testing.T) {
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()
	ctx := context.Background()

	collector, err := NewIOKitBlockDeviceStatsCollector(ctx, "/", 50*time.Millisecond, &logger, time.Now)
	if err != nil {
		t.Fatalf("NewIOKitBlockDeviceStatsCollector failed: %v", err)
	}

	// Wait for some polling to occur
	time.Sleep(100 * time.Millisecond)

	sample1, _ := collector.CollectVolumeStats()
	epoch1 := sample1.Epoch

	// Close the collector
	if err := collector.Close(); err != nil {
		t.Errorf("Close() returned error: %v", err)
	}

	// Wait and verify epoch doesn't change anymore
	time.Sleep(100 * time.Millisecond)

	sample2, _ := collector.CollectVolumeStats()
	epoch2 := sample2.Epoch

	if epoch2 != epoch1 {
		t.Errorf("Epoch should not change after Close: epoch1=%d, epoch2=%d", epoch1, epoch2)
	}
}

func TestIOKitBlockDeviceStatsCollector_ContextCancellation(t *testing.T) {
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()
	ctx, cancel := context.WithCancel(context.Background())

	collector, err := NewIOKitBlockDeviceStatsCollector(ctx, "/", 50*time.Millisecond, &logger, time.Now)
	if err != nil {
		t.Fatalf("NewIOKitBlockDeviceStatsCollector failed: %v", err)
	}
	defer collector.Close()

	// Wait for some polling
	time.Sleep(100 * time.Millisecond)

	sample1, _ := collector.CollectVolumeStats()
	epoch1 := sample1.Epoch

	// Cancel the context
	cancel()

	// Wait and verify polling stops
	time.Sleep(100 * time.Millisecond)

	sample2, _ := collector.CollectVolumeStats()
	epoch2 := sample2.Epoch

	// Epoch should not have changed significantly after cancel
	// Allow for one more poll that might have been in flight
	if epoch2 > epoch1+1 {
		t.Errorf("Polling should stop after context cancellation: epoch1=%d, epoch2=%d", epoch1, epoch2)
	}
}

func TestIOKitBlockDeviceStatsCollector_ConcurrentAccess(t *testing.T) {
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()
	ctx := context.Background()

	collector, err := NewIOKitBlockDeviceStatsCollector(ctx, "/", 20*time.Millisecond, &logger, time.Now)
	if err != nil {
		t.Fatalf("NewIOKitBlockDeviceStatsCollector failed: %v", err)
	}
	defer collector.Close()

	// Run multiple goroutines reading stats concurrently
	done := make(chan struct{})
	errChan := make(chan error, 10)

	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				select {
				case <-done:
					return
				default:
					_, err := collector.CollectVolumeStats()
					if err != nil {
						errChan <- err
						return
					}
				}
			}
		}()
	}

	// Let it run for a bit
	time.Sleep(200 * time.Millisecond)
	close(done)

	// Check for any errors
	select {
	case err := <-errChan:
		t.Errorf("Concurrent access produced error: %v", err)
	default:
		// No errors, good
	}
}

func TestIOKitBlockDeviceStatsCollector_InitialPollOccurs(t *testing.T) {
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()
	ctx := context.Background()

	collector, err := NewIOKitBlockDeviceStatsCollector(ctx, "/", 10*time.Second, &logger, time.Now)
	if err != nil {
		t.Fatalf("NewIOKitBlockDeviceStatsCollector failed: %v", err)
	}
	defer collector.Close()

	// With a 10s poll interval, we should still get an immediate poll
	// Wait a short time for the initial poll to complete
	time.Sleep(100 * time.Millisecond)

	sample, _ := collector.CollectVolumeStats()

	// Epoch should be at least 1 from the initial poll
	if sample.Epoch == 0 {
		t.Error("Initial poll should have occurred, but epoch is 0")
	}

	// Value should be populated
	if sample.Value == nil || sample.Value.Apple == nil {
		t.Error("Initial poll should have populated stats")
	}
}

func TestIOKitBlockDeviceStatsCollector_StatsContainExpectedFields(t *testing.T) {
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()
	ctx := context.Background()

	collector, err := NewIOKitBlockDeviceStatsCollector(ctx, "/", 50*time.Millisecond, &logger, time.Now)
	if err != nil {
		t.Fatalf("NewIOKitBlockDeviceStatsCollector failed: %v", err)
	}
	defer collector.Close()

	// Wait for poll
	time.Sleep(100 * time.Millisecond)

	sample, err := collector.CollectVolumeStats()
	if err != nil {
		t.Fatalf("CollectVolumeStats() returned error: %v", err)
	}

	if sample.Value == nil {
		t.Fatal("sample.Value is nil")
	}

	if sample.Value.Apple == nil {
		t.Fatal("sample.Value.Apple is nil")
	}

	// Linux stats should be nil on darwin
	if sample.Value.Linux != nil {
		t.Error("sample.Value.Linux should be nil on darwin")
	}

	apple := sample.Value.Apple

	// Log all stats for verification
	t.Logf("BytesRead: %d", apple.BytesRead)
	t.Logf("BytesWritten: %d", apple.BytesWritten)
	t.Logf("LatentReadTime: %d", apple.LatentReadTime)
	t.Logf("LatentWriteTime: %d", apple.LatentWriteTime)
	t.Logf("ReadErrors: %d", apple.ReadErrors)
	t.Logf("ReadRetries: %d", apple.ReadRetries)
	t.Logf("Reads: %d", apple.Reads)
	t.Logf("TotalReadTime: %d", apple.TotalReadTime)
	t.Logf("TotalWriteTime: %d", apple.TotalWriteTime)
	t.Logf("WriteErrors: %d", apple.WriteErrors)
	t.Logf("WriteRetries: %d", apple.WriteRetries)
	t.Logf("Writes: %d", apple.Writes)

	// BytesRead and Reads should be > 0 since we're definitely reading from the disk
	// (the test itself reads files)
	if apple.BytesRead == 0 && apple.Reads == 0 {
		t.Log("Warning: BytesRead and Reads are both 0, which is unusual")
	}
}

func TestIOKitBlockDeviceStatsCollector_SampleValid(t *testing.T) {
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()
	ctx := context.Background()

	collector, err := NewIOKitBlockDeviceStatsCollector(ctx, "/", 50*time.Millisecond, &logger, time.Now)
	if err != nil {
		t.Fatalf("NewIOKitBlockDeviceStatsCollector failed: %v", err)
	}
	defer collector.Close()

	// Wait for poll
	time.Sleep(100 * time.Millisecond)

	sample, err := collector.CollectVolumeStats()
	if err != nil {
		t.Fatalf("CollectVolumeStats() returned error: %v", err)
	}

	// The sample should be valid (using the domain.Sample.Valid() logic)
	if sample.Error != nil {
		t.Errorf("sample.Error should be nil, got: %v", sample.Error)
	}
	if sample.Time.IsZero() {
		t.Error("sample.Time should not be zero")
	}
	if sample.Epoch == 0 {
		t.Error("sample.Epoch should not be zero")
	}
}

// Test error sentinel values
func TestIOKitErrors(t *testing.T) {
	// Verify error sentinel values are properly defined
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

	for _, sentinel := range sentinels {
		if sentinel == nil {
			t.Error("Error sentinel should not be nil")
		}
		if sentinel.Error() == "" {
			t.Error("Error sentinel should have non-empty message")
		}
	}

	// Test errors.Is
	wrappedErr := errors.New("wrapped: " + ErrIOReturnNoDevice.Error())
	if errors.Is(wrappedErr, ErrIOReturnNoDevice) {
		t.Error("errors.Is should not match for different error with same message")
	}

	// Test that sentinel errors are distinct
	if errors.Is(ErrIOReturnNoDevice, ErrIOReturnError) {
		t.Error("Different sentinel errors should not match")
	}
}

// Benchmark tests

func BenchmarkCollectVolumeStats(b *testing.B) {
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()
	ctx := context.Background()

	collector, err := NewIOKitBlockDeviceStatsCollector(ctx, "/", 1*time.Hour, &logger, time.Now)
	if err != nil {
		b.Fatalf("NewIOKitBlockDeviceStatsCollector failed: %v", err)
	}
	defer collector.Close()

	// Wait for initial poll
	time.Sleep(100 * time.Millisecond)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = collector.CollectVolumeStats()
	}
}

func BenchmarkNewIOKitBlockDeviceStatsCollector(b *testing.B) {
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()
	ctx := context.Background()

	for i := 0; i < b.N; i++ {
		collector, err := NewIOKitBlockDeviceStatsCollector(ctx, "/", 1*time.Hour, &logger, time.Now)
		if err != nil {
			b.Fatalf("NewIOKitBlockDeviceStatsCollector failed: %v", err)
		}
		collector.Close()
	}
}
