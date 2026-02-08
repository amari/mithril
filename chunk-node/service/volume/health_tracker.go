package volume

import (
	"context"
	"sync"
	"time"

	"github.com/amari/mithril/chunk-node/domain"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// VolumeHealthTracker implements VolumeHealthChecker with circuit breaker logic.
// It subscribes to stats from VolumeStatsManager and tracks operation errors
// to manage health state transitions.
type VolumeHealthTracker struct {
	statsManager      *VolumeStatsManager
	attributeRegistry portvolume.VolumeAttributeRegistry
	log               *zerolog.Logger
	now               func() time.Time

	// OTel metrics
	healthStateGauge       metric.Int64Gauge
	transitionsCounter     metric.Int64Counter
	operationErrorsCounter metric.Int64Counter
	stuckEpochsGauge       metric.Int64Gauge
	availableSpaceGauge    metric.Int64Gauge
	timeInStateHistogram   metric.Float64Histogram

	mu      sync.RWMutex
	states  map[domain.VolumeID]*volumeHealthState
	watches map[domain.VolumeID]*healthWatch
}

var _ portvolume.VolumeHealthChecker = (*VolumeHealthTracker)(nil)

type volumeHealthState struct {
	state                  domain.VolumeState
	config                 HealthConfig
	errorCount             int       // Consecutive errors in current state
	successCount           int       // Consecutive successes (for recovery)
	lastTransition         time.Time // Last state transition time
	lastFatalError         error     // Most recent fatal error
	consecutiveFatalErrors int       // Consecutive fatal errors
	availableSpaceBytes    int64     // Cached from last stats check
	totalErrorCount        int       // Total errors recorded (for VolumeHealth.ErrorCount)

	// Track previous samples to detect staleness (Epoch advancing but Time frozen)
	lastSpaceUtilizationSample *domain.Sample[*domain.SpaceUtilizationStats]
	lastBlockDeviceSample      *domain.Sample[*domain.BlockDeviceStats]
}

type healthWatch struct {
	volumeID domain.VolumeID
	cancel   context.CancelFunc
	done     chan struct{}
}

// HealthConfig defines per-volume health thresholds.
type HealthConfig struct {
	// DegradedThreshold - errors before Ok → Degraded
	DegradedThreshold int

	// FailedThreshold - consecutive fatal errors before → Failed
	FailedThreshold int

	// RecoverySuccessCount - consecutive successes needed for Degraded → Ok
	RecoverySuccessCount int

	// RecoveryWindow - time window for recovery attempts
	RecoveryWindow time.Duration

	// Stats-based thresholds
	SpaceUsageThreshold float64       // 0.0-1.0, e.g., 0.9 = 90% full → Degraded
	LatencyThreshold    time.Duration // Average latency threshold

	// Staleness detection
	StuckEpochThreshold uint64 // Degrade after N epochs stuck, default: 3
}

func NewVolumeHealthTracker(
	statsManager *VolumeStatsManager,
	attributeRegistry portvolume.VolumeAttributeRegistry,
	meter metric.Meter,
	log *zerolog.Logger,
) (*VolumeHealthTracker, error) {
	// Create OTel metrics
	healthStateGauge, err := meter.Int64Gauge("volume.health.state",
		metric.WithDescription("Current health state of volume (0=unknown, 1=ok, 2=degraded, 3=failed)"),
		metric.WithUnit("{state}"))
	if err != nil {
		return nil, err
	}

	transitionsCounter, err := meter.Int64Counter("volume.health.transitions",
		metric.WithDescription("Count of health state transitions"),
		metric.WithUnit("{transition}"))
	if err != nil {
		return nil, err
	}

	operationErrorsCounter, err := meter.Int64Counter("volume.operation.errors",
		metric.WithDescription("Count of volume operation errors by classification"),
		metric.WithUnit("{error}"))
	if err != nil {
		return nil, err
	}

	stuckEpochsGauge, err := meter.Int64Gauge("volume.stats.stuck_epochs",
		metric.WithDescription("Number of epochs a stats subsystem has been stuck"),
		metric.WithUnit("{epoch}"))
	if err != nil {
		return nil, err
	}

	availableSpaceGauge, err := meter.Int64Gauge("volume.available_space_bytes",
		metric.WithDescription("Available space on volume in bytes"),
		metric.WithUnit("By"))
	if err != nil {
		return nil, err
	}

	timeInStateHistogram, err := meter.Float64Histogram("volume.health.state_duration",
		metric.WithDescription("Duration in seconds that volume spent in a health state"),
		metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}

	return &VolumeHealthTracker{
		statsManager:           statsManager,
		attributeRegistry:      attributeRegistry,
		log:                    log,
		now:                    time.Now,
		healthStateGauge:       healthStateGauge,
		transitionsCounter:     transitionsCounter,
		operationErrorsCounter: operationErrorsCounter,
		stuckEpochsGauge:       stuckEpochsGauge,
		availableSpaceGauge:    availableSpaceGauge,
		timeInStateHistogram:   timeInStateHistogram,
		states:                 make(map[domain.VolumeID]*volumeHealthState),
		watches:                make(map[domain.VolumeID]*healthWatch),
	}, nil
}

// buildAttributes creates metric attributes for a volume
func (t *VolumeHealthTracker) buildAttributes(volumeID domain.VolumeID, extraAttrs ...attribute.KeyValue) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.Int("volume.id", int(volumeID)),
	}

	// Add registered static attributes
	if registeredAttrs := t.attributeRegistry.GetAttributes(volumeID); registeredAttrs != nil {
		attrs = append(attrs, registeredAttrs...)
	}

	// Add any extra attributes
	if len(extraAttrs) > 0 {
		attrs = append(attrs, extraAttrs...)
	}

	return attrs
}

// recordStateTransition records a health state transition in metrics
func (t *VolumeHealthTracker) recordStateTransition(volumeID domain.VolumeID, from, to domain.VolumeState) {
	ctx := context.Background()

	// Record transition counter
	attrs := t.buildAttributes(volumeID,
		attribute.String("from_state", from.String()),
		attribute.String("to_state", to.String()),
	)
	t.transitionsCounter.Add(ctx, 1, metric.WithAttributes(attrs...))

	// Record new state gauge
	stateAttrs := t.buildAttributes(volumeID)
	t.healthStateGauge.Record(ctx, int64(to), metric.WithAttributes(stateAttrs...))
}

// recordTimeInState records how long was spent in a state (when leaving it)
func (t *VolumeHealthTracker) recordTimeInState(volumeID domain.VolumeID, state domain.VolumeState, duration time.Duration) {
	ctx := context.Background()
	attrs := t.buildAttributes(volumeID,
		attribute.String("state", state.String()),
	)
	t.timeInStateHistogram.Record(ctx, duration.Seconds(), metric.WithAttributes(attrs...))
}

// AddVolume starts health tracking for a volume.
// Internally subscribes to stats updates from VolumeStatsManager.
func (t *VolumeHealthTracker) AddVolume(volumeID domain.VolumeID, config HealthConfig) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Check if already tracking
	if _, exists := t.states[volumeID]; exists {
		return nil
	}

	// Apply defaults for zero values
	if config.DegradedThreshold == 0 {
		config.DegradedThreshold = 3
	}
	if config.FailedThreshold == 0 {
		config.FailedThreshold = 2
	}
	if config.RecoverySuccessCount == 0 {
		config.RecoverySuccessCount = 10
	}
	if config.RecoveryWindow == 0 {
		config.RecoveryWindow = 5 * time.Minute
	}
	if config.SpaceUsageThreshold == 0 {
		config.SpaceUsageThreshold = 0.90
	}
	if config.LatencyThreshold == 0 {
		config.LatencyThreshold = 500 * time.Millisecond
	}
	if config.StuckEpochThreshold == 0 {
		config.StuckEpochThreshold = 3
	}

	// Initialize state
	state := &volumeHealthState{
		state:          domain.VolumeStateOK,
		config:         config,
		lastTransition: t.now(),
	}
	t.states[volumeID] = state

	// Record initial state
	ctx := context.Background()
	attrs := t.buildAttributes(volumeID)
	t.healthStateGauge.Record(ctx, int64(domain.VolumeStateOK), metric.WithAttributes(attrs...))

	// Start watching stats
	watchCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	watch := &healthWatch{
		volumeID: volumeID,
		cancel:   cancel,
		done:     done,
	}
	t.watches[volumeID] = watch

	// Subscribe to stats
	statsCh, err := t.statsManager.WatchVolume(watchCtx, volumeID)
	if err != nil {
		// Stats manager doesn't have this volume yet, that's ok
		// Health tracking will still work based on operation errors
		t.log.Debug().
			Err(err).
			Uint16("volume_id", uint16(volumeID)).
			Msg("could not watch volume stats, will track health via operations only")
		close(done)
		return nil
	}

	// Start goroutine to process stats updates
	go t.processStatsUpdates(watchCtx, watch, statsCh)

	t.log.Info().
		Uint16("volume_id", uint16(volumeID)).
		Msg("started volume health tracking")

	return nil
}

// RemoveVolume stops health tracking for a volume.
func (t *VolumeHealthTracker) RemoveVolume(volumeID domain.VolumeID) error {
	t.mu.Lock()
	watch, exists := t.watches[volumeID]
	state := t.states[volumeID]
	if !exists {
		t.mu.Unlock()
		return nil
	}
	delete(t.watches, volumeID)
	delete(t.states, volumeID)
	t.mu.Unlock()

	// Record time spent in final state
	if state != nil {
		duration := t.now().Sub(state.lastTransition)
		t.recordTimeInState(volumeID, state.state, duration)
	}

	// Cancel and wait for goroutine to finish
	watch.cancel()
	<-watch.done

	t.log.Info().
		Uint16("volume_id", uint16(volumeID)).
		Msg("stopped volume health tracking")

	return nil
}

// ClearVolumes stops health tracking for all volumes.
func (t *VolumeHealthTracker) ClearVolumes() {
	t.mu.Lock()
	watches := make([]*healthWatch, 0, len(t.watches))
	states := make(map[domain.VolumeID]*volumeHealthState)
	for vid, watch := range t.watches {
		watches = append(watches, watch)
		if state := t.states[vid]; state != nil {
			states[vid] = state
		}
	}
	t.watches = make(map[domain.VolumeID]*healthWatch)
	t.states = make(map[domain.VolumeID]*volumeHealthState)
	t.mu.Unlock()

	// Record time in state for all volumes
	now := t.now()
	for vid, state := range states {
		duration := now.Sub(state.lastTransition)
		t.recordTimeInState(vid, state.state, duration)
	}

	// Cancel all watches
	for _, watch := range watches {
		watch.cancel()
		<-watch.done
	}

	t.log.Info().Msg("cleared all volume health tracking")
}

// CheckVolumeHealth implements VolumeHealthChecker interface.
func (t *VolumeHealthTracker) CheckVolumeHealth(volumeID domain.VolumeID) *domain.VolumeHealth {
	t.mu.RLock()
	defer t.mu.RUnlock()

	state, exists := t.states[volumeID]
	if !exists {
		// Volume not tracked, return default
		return &domain.VolumeHealth{
			State: domain.VolumeStateUnknown,
		}
	}

	return &domain.VolumeHealth{
		State: state.state,
	}
}

// RecordError records an operation error and potentially transitions state.
// Called by handlers after volume operations fail.
func (t *VolumeHealthTracker) RecordError(ctx context.Context, volumeID domain.VolumeID, err error) domain.VolumeState {
	t.mu.Lock()
	defer t.mu.Unlock()

	state, exists := t.states[volumeID]
	if !exists {
		// Volume not tracked, can't update health
		return domain.VolumeStateUnknown
	}

	errorClass := classifyVolumeError(err)

	// Record error metric
	errorClassStr := ""
	switch errorClass {
	case VolumeErrorClassFatal:
		errorClassStr = "fatal"
	case VolumeErrorClassDegrading:
		errorClassStr = "degrading"
	case VolumeErrorClassTransient:
		errorClassStr = "transient"
	}

	attrs := t.buildAttributes(volumeID,
		attribute.String("error_class", errorClassStr),
	)
	t.operationErrorsCounter.Add(ctx, 1, metric.WithAttributes(attrs...))

	// Increment total error count
	state.totalErrorCount++

	// Reset success counter on any error
	state.successCount = 0

	switch errorClass {
	case VolumeErrorClassFatal:
		return t.handleFatalError(volumeID, state, err)

	case VolumeErrorClassDegrading:
		return t.handleDegradingError(volumeID, state, err)

	case VolumeErrorClassTransient:
		// No state change for transient errors
		return state.state
	}

	return state.state
}

// RecordSuccess records a successful operation.
// May contribute to Degraded → Ok transition based on circuit breaker recovery.
func (t *VolumeHealthTracker) RecordSuccess(ctx context.Context, volumeID domain.VolumeID) {
	t.mu.Lock()
	defer t.mu.Unlock()

	state, exists := t.states[volumeID]
	if !exists {
		return
	}

	// Reset error counters
	state.errorCount = 0
	state.consecutiveFatalErrors = 0

	// Only track success count for recovery from Degraded
	if state.state == domain.VolumeStateDegraded {
		state.successCount++

		// Check recovery criteria
		if state.successCount >= state.config.RecoverySuccessCount &&
			time.Since(state.lastTransition) >= state.config.RecoveryWindow {

			t.log.Info().
				Uint16("volume_id", uint16(volumeID)).
				Int("success_count", state.successCount).
				Dur("recovery_duration", time.Since(state.lastTransition)).
				Msg("volume recovered from degraded state")

			// Record time in degraded state
			duration := t.now().Sub(state.lastTransition)
			t.recordTimeInState(volumeID, domain.VolumeStateDegraded, duration)

			// Transition to OK
			oldState := state.state
			state.state = domain.VolumeStateOK
			state.lastTransition = t.now()
			state.successCount = 0

			// Record transition
			t.recordStateTransition(volumeID, oldState, domain.VolumeStateOK)
		}
	}
}

// SetConfig updates the health thresholds for a specific volume.
func (t *VolumeHealthTracker) SetConfig(volumeID domain.VolumeID, config HealthConfig) {
	t.mu.Lock()
	defer t.mu.Unlock()

	state, exists := t.states[volumeID]
	if !exists {
		return
	}

	// Apply defaults for zero values
	if config.DegradedThreshold == 0 {
		config.DegradedThreshold = 3
	}
	if config.FailedThreshold == 0 {
		config.FailedThreshold = 2
	}
	if config.RecoverySuccessCount == 0 {
		config.RecoverySuccessCount = 10
	}
	if config.RecoveryWindow == 0 {
		config.RecoveryWindow = 5 * time.Minute
	}
	if config.SpaceUsageThreshold == 0 {
		config.SpaceUsageThreshold = 0.90
	}
	if config.LatencyThreshold == 0 {
		config.LatencyThreshold = 500 * time.Millisecond
	}
	if config.StuckEpochThreshold == 0 {
		config.StuckEpochThreshold = 3
	}

	state.config = config

	t.log.Debug().
		Uint16("volume_id", uint16(volumeID)).
		Interface("config", config).
		Msg("updated volume health config")
}

// processStatsUpdates is the goroutine that receives stats and updates health
func (t *VolumeHealthTracker) processStatsUpdates(ctx context.Context, watch *healthWatch, statsCh <-chan domain.VolumeStats) {
	defer close(watch.done)

	for {
		select {
		case <-ctx.Done():
			return
		case stats, ok := <-statsCh:
			if !ok {
				// Stats channel closed
				return
			}
			t.updateHealthFromStats(watch.volumeID, stats)
		}
	}
}

// updateHealthFromStats checks stats-based health triggers
func (t *VolumeHealthTracker) updateHealthFromStats(volumeID domain.VolumeID, stats domain.VolumeStats) {
	t.mu.Lock()
	defer t.mu.Unlock()

	state, exists := t.states[volumeID]
	if !exists {
		return
	}

	ctx := context.Background()

	// Skip stats checks for Failed volumes
	if state.state == domain.VolumeStateFailed {
		return
	}

	// Check for stale SpaceUtilization (statfs stuck)
	if state.lastSpaceUtilizationSample != nil {
		lastSample := state.lastSpaceUtilizationSample
		currentSample := stats.SpaceUtilization

		// Epoch advanced but Time didn't change (or went backwards) → syscall is stuck/timing out
		if currentSample.Epoch > lastSample.Epoch &&
			!currentSample.Time.After(lastSample.Time) {

			staleDuration := t.now().Sub(currentSample.Time)
			epochsStuck := currentSample.Epoch - lastSample.Epoch

			// Record stuck epochs metric
			attrs := t.buildAttributes(volumeID,
				attribute.String("subsystem", "statfs"),
			)
			t.stuckEpochsGauge.Record(ctx, int64(epochsStuck), metric.WithAttributes(attrs...))

			t.log.Warn().
				Uint16("volume_id", uint16(volumeID)).
				Uint64("epochs_stuck", epochsStuck).
				Dur("stale_duration", staleDuration).
				Time("frozen_since", currentSample.Time).
				Msg("statfs appears stuck (epoch advancing but time frozen)")

			// Degrade if stuck for configured threshold
			if state.state == domain.VolumeStateOK && epochsStuck >= state.config.StuckEpochThreshold {
				t.log.Warn().
					Uint16("volume_id", uint16(volumeID)).
					Msg("volume degraded due to stuck statfs")

				// Record time in OK state
				duration := t.now().Sub(state.lastTransition)
				t.recordTimeInState(volumeID, domain.VolumeStateOK, duration)

				oldState := state.state
				state.state = domain.VolumeStateDegraded
				state.lastTransition = t.now()

				// Record transition
				t.recordStateTransition(volumeID, oldState, domain.VolumeStateDegraded)
			}
		} else if currentSample.Epoch > lastSample.Epoch {
			// Not stuck anymore, record 0 stuck epochs
			attrs := t.buildAttributes(volumeID,
				attribute.String("subsystem", "statfs"),
			)
			t.stuckEpochsGauge.Record(ctx, 0, metric.WithAttributes(attrs...))
		}
	}

	// Check for stale BlockDevice (IOKit/sysfs stuck)
	if state.lastBlockDeviceSample != nil {
		lastSample := state.lastBlockDeviceSample
		currentSample := stats.BlockDevice

		// Epoch advanced but Time didn't change (or went backwards) → syscall is stuck/timing out
		if currentSample.Epoch > lastSample.Epoch &&
			!currentSample.Time.After(lastSample.Time) {

			staleDuration := t.now().Sub(currentSample.Time)
			epochsStuck := currentSample.Epoch - lastSample.Epoch

			// Record stuck epochs metric
			attrs := t.buildAttributes(volumeID,
				attribute.String("subsystem", "block_device"),
			)
			t.stuckEpochsGauge.Record(ctx, int64(epochsStuck), metric.WithAttributes(attrs...))

			t.log.Warn().
				Uint16("volume_id", uint16(volumeID)).
				Uint64("epochs_stuck", epochsStuck).
				Dur("stale_duration", staleDuration).
				Time("frozen_since", currentSample.Time).
				Msg("block device stats stuck (epoch advancing but time frozen)")

			// Degrade if stuck for configured threshold
			if state.state == domain.VolumeStateOK && epochsStuck >= state.config.StuckEpochThreshold {
				t.log.Warn().
					Uint16("volume_id", uint16(volumeID)).
					Msg("volume degraded due to stuck block device stats")

				// Record time in OK state
				duration := t.now().Sub(state.lastTransition)
				t.recordTimeInState(volumeID, domain.VolumeStateOK, duration)

				oldState := state.state
				state.state = domain.VolumeStateDegraded
				state.lastTransition = t.now()

				// Record transition
				t.recordStateTransition(volumeID, oldState, domain.VolumeStateDegraded)
			}
		} else if currentSample.Epoch > lastSample.Epoch {
			// Not stuck anymore, record 0 stuck epochs
			attrs := t.buildAttributes(volumeID,
				attribute.String("subsystem", "block_device"),
			)
			t.stuckEpochsGauge.Record(ctx, 0, metric.WithAttributes(attrs...))
		}
	}

	// Cache current samples for next comparison
	state.lastSpaceUtilizationSample = &stats.SpaceUtilization
	state.lastBlockDeviceSample = &stats.BlockDevice

	// Update available space
	if stats.SpaceUtilization.Valid() {
		state.availableSpaceBytes = stats.SpaceUtilization.Value.FreeBytes

		// Record available space metric
		attrs := t.buildAttributes(volumeID)
		t.availableSpaceGauge.Record(ctx, stats.SpaceUtilization.Value.FreeBytes,
			metric.WithAttributes(attrs...))

		// Check space usage threshold
		total := stats.SpaceUtilization.Value.TotalBytes
		used := stats.SpaceUtilization.Value.UsedBytes
		if total > 0 {
			usageRatio := float64(used) / float64(total)

			if usageRatio >= state.config.SpaceUsageThreshold {
				// Space usage above threshold → Degraded
				if state.state == domain.VolumeStateOK {
					t.log.Warn().
						Uint16("volume_id", uint16(volumeID)).
						Float64("usage", usageRatio).
						Float64("threshold", state.config.SpaceUsageThreshold).
						Int64("free_bytes", stats.SpaceUtilization.Value.FreeBytes).
						Msg("volume degraded due to high space usage")

					// Record time in OK state
					duration := t.now().Sub(state.lastTransition)
					t.recordTimeInState(volumeID, domain.VolumeStateOK, duration)

					oldState := state.state
					state.state = domain.VolumeStateDegraded
					state.lastTransition = t.now()

					// Record transition
					t.recordStateTransition(volumeID, oldState, domain.VolumeStateDegraded)
				}
			} else if usageRatio < state.config.SpaceUsageThreshold-0.05 {
				// Space usage dropped below threshold with hysteresis → may recover
				if state.state == domain.VolumeStateDegraded && time.Since(state.lastTransition) > state.config.RecoveryWindow {
					t.log.Info().
						Uint16("volume_id", uint16(volumeID)).
						Float64("usage", usageRatio).
						Msg("volume recovered from space-based degradation")

					// Record time in degraded state
					duration := t.now().Sub(state.lastTransition)
					t.recordTimeInState(volumeID, domain.VolumeStateDegraded, duration)

					oldState := state.state
					state.state = domain.VolumeStateOK
					state.lastTransition = t.now()

					// Record transition
					t.recordStateTransition(volumeID, oldState, domain.VolumeStateOK)
				}
			}
		}
	}

	// Check latency threshold (if block device stats available)
	if stats.BlockDevice.Valid() && state.config.LatencyThreshold > 0 {
		var avgLatency time.Duration

		if stats.BlockDevice.Value.Apple != nil {
			apple := stats.BlockDevice.Value.Apple
			totalOps := apple.Reads + apple.Writes
			if totalOps > 0 {
				totalLatency := apple.LatentReadTime + apple.LatentWriteTime
				avgLatency = time.Duration(totalLatency / totalOps)
			}
		} else if stats.BlockDevice.Value.Linux != nil {
			linux := stats.BlockDevice.Value.Linux
			totalOps := linux.Reads + linux.Writes
			if totalOps > 0 {
				totalTicks := linux.ReadTicks + linux.WriteTicks
				avgLatency = time.Duration(totalTicks/totalOps) * time.Millisecond
			}
		}

		if avgLatency > state.config.LatencyThreshold {
			if state.state == domain.VolumeStateOK {
				t.log.Warn().
					Uint16("volume_id", uint16(volumeID)).
					Dur("avg_latency", avgLatency).
					Dur("threshold", state.config.LatencyThreshold).
					Msg("volume degraded due to high latency")

				// Record time in OK state
				duration := t.now().Sub(state.lastTransition)
				t.recordTimeInState(volumeID, domain.VolumeStateOK, duration)

				oldState := state.state
				state.state = domain.VolumeStateDegraded
				state.lastTransition = t.now()

				// Record transition
				t.recordStateTransition(volumeID, oldState, domain.VolumeStateDegraded)
			}
		}
	}
}

func (t *VolumeHealthTracker) handleFatalError(volumeID domain.VolumeID, state *volumeHealthState, err error) domain.VolumeState {
	state.consecutiveFatalErrors++
	state.lastFatalError = err

	// Transition to Failed after threshold consecutive fatal errors
	if state.consecutiveFatalErrors >= state.config.FailedThreshold {
		if state.state != domain.VolumeStateFailed {
			t.log.Error().
				Uint16("volume_id", uint16(volumeID)).
				Err(err).
				Int("consecutive_fatal_errors", state.consecutiveFatalErrors).
				Msg("volume failed due to fatal errors")

			// Record time in previous state
			duration := t.now().Sub(state.lastTransition)
			t.recordTimeInState(volumeID, state.state, duration)

			oldState := state.state
			state.state = domain.VolumeStateFailed
			state.lastTransition = t.now()

			// Record transition
			t.recordStateTransition(volumeID, oldState, domain.VolumeStateFailed)
		}
	}

	return state.state
}

func (t *VolumeHealthTracker) handleDegradingError(volumeID domain.VolumeID, state *volumeHealthState, err error) domain.VolumeState {
	state.errorCount++

	// Transition to Degraded after threshold errors
	if state.state == domain.VolumeStateOK && state.errorCount >= state.config.DegradedThreshold {
		t.log.Warn().
			Uint16("volume_id", uint16(volumeID)).
			Err(err).
			Int("error_count", state.errorCount).
			Msg("volume degraded due to errors")

		// Record time in OK state
		duration := t.now().Sub(state.lastTransition)
		t.recordTimeInState(volumeID, domain.VolumeStateOK, duration)

		oldState := state.state
		state.state = domain.VolumeStateDegraded
		state.lastTransition = t.now()
		state.errorCount = 0 // Reset for next threshold check

		// Record transition
		t.recordStateTransition(volumeID, oldState, domain.VolumeStateDegraded)
	}

	return state.state
}

func defaultHealthConfig() HealthConfig {
	return HealthConfig{
		DegradedThreshold:    3,                      // 3 degrading errors → Degraded
		FailedThreshold:      2,                      // 2 fatal errors → Failed
		RecoverySuccessCount: 10,                     // 10 successes to recover
		RecoveryWindow:       5 * time.Minute,        // Must stay healthy for 5min
		SpaceUsageThreshold:  0.90,                   // 90% full → Degraded
		LatencyThreshold:     500 * time.Millisecond, // Avg latency > 500ms → Degraded
		StuckEpochThreshold:  3,                      // 3 epochs stuck → Degraded
	}
}
