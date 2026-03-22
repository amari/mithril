package adaptersfilestore

import (
	"context"
	"sync"
	"time"

	"github.com/amari/mithril/mithril-node-go/internal/domain"
)

type VolumeHealthSource interface {
	Get() domain.VolumeHealth
}

type SpaceUtilizationSampleVolumeHealthSource struct {
	statisticsProvider domain.VolumeStatisticsProvider[domain.SpaceUtilizationStatistics]

	mu             sync.RWMutex
	health         domain.VolumeHealth
	watchCtx       context.Context
	cancelWatchCtx context.CancelFunc
}

var _ VolumeHealthSource = (*SpaceUtilizationSampleVolumeHealthSource)(nil)

func NewSpaceUtilizationSampleVolumeHealthSource(statisticsProvider domain.VolumeStatisticsProvider[domain.SpaceUtilizationStatistics]) *SpaceUtilizationSampleVolumeHealthSource {
	return &SpaceUtilizationSampleVolumeHealthSource{
		statisticsProvider: statisticsProvider,
		health:             domain.VolumeUnknown,
	}
}

func (s *SpaceUtilizationSampleVolumeHealthSource) start() error {
	s.watchCtx, s.cancelWatchCtx = context.WithCancel(context.Background())
	w := s.statisticsProvider.Watch(s.watchCtx)

	go func() {
		for {
			select {
			case <-w:
			case <-s.watchCtx.Done():
				return
			}

			func() {
				sample := s.statisticsProvider.Get()

				s.handleSample(sample)
			}()
		}
	}()

	return nil
}

func (s *SpaceUtilizationSampleVolumeHealthSource) stop() error {
	s.cancelWatchCtx()

	return nil
}

func (s *SpaceUtilizationSampleVolumeHealthSource) handleSample(sample domain.Sample[domain.SpaceUtilizationStatistics]) {
	if sample.Epoch == 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	switch s.health {
	case domain.VolumeFailed:
		// failed volume stays failed
		return
	case domain.VolumeDegraded:
		// must drop down to 85% used before recovering b/c of 5% hysteresis
		if sample.Value.UsedBytes < (17*sample.Value.TotalBytes)/20 {
			s.health = domain.VolumeOK
		}
	default:
		// past 90% used => degraded
		if sample.Value.UsedBytes > (9*sample.Value.TotalBytes)/10 {
			s.health = domain.VolumeDegraded
		}
	}
}

func (s *SpaceUtilizationSampleVolumeHealthSource) Get() domain.VolumeHealth {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.health
}

type IOKitIOBlockStorageDriverStatisticsVolumeHealthSource struct {
	statisticsProvider domain.VolumeStatisticsProvider[domain.IOKitIOBlockStorageDriverStatistics]

	mu             sync.RWMutex
	health         domain.VolumeHealth
	watchCtx       context.Context
	cancelWatchCtx context.CancelFunc

	sampleWindow []*domain.Sample[domain.IOKitIOBlockStorageDriverStatistics]
	lastSample   *domain.Sample[domain.IOKitIOBlockStorageDriverStatistics]

	degradedAt time.Time
}

func NewIOKitIOBlockStorageDriverStatisticsVolumeHealthSource(
	provider domain.VolumeStatisticsProvider[domain.IOKitIOBlockStorageDriverStatistics],
) *IOKitIOBlockStorageDriverStatisticsVolumeHealthSource {
	return &IOKitIOBlockStorageDriverStatisticsVolumeHealthSource{
		statisticsProvider: provider,
		sampleWindow:       make([]*domain.Sample[domain.IOKitIOBlockStorageDriverStatistics], 0, 10),
	}
}

var _ VolumeHealthSource = (*IOKitIOBlockStorageDriverStatisticsVolumeHealthSource)(nil)

func (s *IOKitIOBlockStorageDriverStatisticsVolumeHealthSource) start() error {
	if s.watchCtx != nil {
		return nil // already started
	}

	s.watchCtx, s.cancelWatchCtx = context.WithCancel(context.Background())
	w := s.statisticsProvider.Watch(s.watchCtx)

	go func() {
		for {
			select {
			case <-w:
				sample := s.statisticsProvider.Get()
				s.handleSample(&sample)

			case <-s.watchCtx.Done():
				return
			}
		}
	}()

	return nil
}

func (s *IOKitIOBlockStorageDriverStatisticsVolumeHealthSource) stop() error {
	if s.cancelWatchCtx != nil {
		s.cancelWatchCtx()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.watchCtx = nil
	s.cancelWatchCtx = nil
	s.sampleWindow = s.sampleWindow[:0]
	s.lastSample = nil

	return nil
}

func (s *IOKitIOBlockStorageDriverStatisticsVolumeHealthSource) handleSample(
	sample *domain.Sample[domain.IOKitIOBlockStorageDriverStatistics],
) {
	if sample.Epoch == 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// First sample
	if s.lastSample == nil {
		s.lastSample = sample
		s.sampleWindow = append(s.sampleWindow, sample)
		return
	}

	// Duplicate sample
	if sample.Epoch == s.lastSample.Epoch {
		return
	}

	// Sliding window
	if len(s.sampleWindow) >= 10 {
		s.sampleWindow = s.sampleWindow[1:]
	}
	s.sampleWindow = append(s.sampleWindow, sample)
	s.lastSample = sample

	// Compute average latencies between adjacent samples
	readLatencies := make([]time.Duration, 0, len(s.sampleWindow))
	writeLatencies := make([]time.Duration, 0, len(s.sampleWindow))

	for i := 0; i < len(s.sampleWindow)-1; i++ {
		left := s.sampleWindow[i]
		right := s.sampleWindow[i+1]

		// Compute deltas
		dReadTime := right.Value.LatentReadTime - left.Value.LatentReadTime
		dReads := right.Value.Reads - left.Value.Reads

		dWriteTime := right.Value.LatentWriteTime - left.Value.LatentWriteTime
		dWrites := right.Value.Writes - left.Value.Writes

		// Guard against resets or negative deltas
		if dReadTime >= 0 && dReads > 0 {
			readLatencies = append(readLatencies, time.Duration(dReadTime/int64(dReads)))
		}
		if dWriteTime >= 0 && dWrites > 0 {
			writeLatencies = append(writeLatencies, time.Duration(dWriteTime/int64(dWrites)))
		}
	}

	// No usable data
	if len(readLatencies) == 0 && len(writeLatencies) == 0 {
		return
	}

	s.updateHealth(readLatencies, writeLatencies)
}

func (s *IOKitIOBlockStorageDriverStatisticsVolumeHealthSource) updateHealth(
	readLatencies, writeLatencies []time.Duration,
) {
	switch s.health {

	case domain.VolumeFailed:
		return

	case domain.VolumeDegraded:
		// Stay degraded for at least 5 seconds
		if time.Since(s.degradedAt) < 5*time.Second {
			return
		}

		// Look at last 5 samples
		if len(readLatencies) > 5 {
			readLatencies = readLatencies[len(readLatencies)-5:]
		}
		if len(writeLatencies) > 5 {
			writeLatencies = writeLatencies[len(writeLatencies)-5:]
		}

		// Recovery threshold
		for _, r := range readLatencies {
			if r > 80*time.Millisecond {
				return
			}
		}
		for _, w := range writeLatencies {
			if w > 80*time.Millisecond {
				return
			}
		}

		s.health = domain.VolumeOK
		return

	default:
		// OK → Degraded transition
		badReads := 0
		badWrites := 0

		for _, r := range readLatencies {
			if r > 100*time.Millisecond {
				badReads++
			}
		}
		for _, w := range writeLatencies {
			if w > 100*time.Millisecond {
				badWrites++
			}
		}

		if badReads >= 3 || badWrites >= 3 {
			s.health = domain.VolumeDegraded
			s.degradedAt = time.Now()
		}
	}
}

func (s *IOKitIOBlockStorageDriverStatisticsVolumeHealthSource) Get() domain.VolumeHealth {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.health
}

type LinuxBlockLayerStatisticsVolumeHealthSource struct {
	statisticsProvider domain.VolumeStatisticsProvider[domain.LinuxBlockLayerStatistics]

	mu             sync.RWMutex
	health         domain.VolumeHealth
	watchCtx       context.Context
	cancelWatchCtx context.CancelFunc

	sampleWindow []*domain.Sample[domain.LinuxBlockLayerStatistics]
	lastSample   *domain.Sample[domain.LinuxBlockLayerStatistics]

	degradedAt time.Time
}

func NewLinuxBlockLayerStatisticsVolumeHealthSource(
	provider domain.VolumeStatisticsProvider[domain.LinuxBlockLayerStatistics],
) *LinuxBlockLayerStatisticsVolumeHealthSource {
	return &LinuxBlockLayerStatisticsVolumeHealthSource{
		statisticsProvider: provider,
		sampleWindow:       make([]*domain.Sample[domain.LinuxBlockLayerStatistics], 0, 10),
	}
}

var _ VolumeHealthSource = (*LinuxBlockLayerStatisticsVolumeHealthSource)(nil)

func (s *LinuxBlockLayerStatisticsVolumeHealthSource) start() error {
	if s.watchCtx != nil {
		return nil // already started
	}

	s.watchCtx, s.cancelWatchCtx = context.WithCancel(context.Background())
	w := s.statisticsProvider.Watch(s.watchCtx)

	go func() {
		for {
			select {
			case <-w:
				sample := s.statisticsProvider.Get()
				s.handleSample(&sample)

			case <-s.watchCtx.Done():
				return
			}
		}
	}()

	return nil
}

func (s *LinuxBlockLayerStatisticsVolumeHealthSource) stop() error {
	if s.cancelWatchCtx != nil {
		s.cancelWatchCtx()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.watchCtx = nil
	s.cancelWatchCtx = nil
	s.sampleWindow = s.sampleWindow[:0]
	s.lastSample = nil

	return nil
}

func (s *LinuxBlockLayerStatisticsVolumeHealthSource) handleSample(
	sample *domain.Sample[domain.LinuxBlockLayerStatistics],
) {
	if sample.Epoch == 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// First sample
	if s.lastSample == nil {
		s.lastSample = sample
		s.sampleWindow = append(s.sampleWindow, sample)
		return
	}

	// Duplicate sample
	if sample.Epoch == s.lastSample.Epoch {
		return
	}

	// Sliding window
	if len(s.sampleWindow) >= 10 {
		s.sampleWindow = s.sampleWindow[1:]
	}
	s.sampleWindow = append(s.sampleWindow, sample)
	s.lastSample = sample

	// Compute average latencies
	readLatencies := make([]time.Duration, 0, len(s.sampleWindow))
	writeLatencies := make([]time.Duration, 0, len(s.sampleWindow))

	for i := 0; i < len(s.sampleWindow)-1; i++ {
		left := s.sampleWindow[i]
		right := s.sampleWindow[i+1]

		// Deltas
		dReadTicks := right.Value.ReadTicks - left.Value.ReadTicks
		dReads := right.Value.Reads - left.Value.Reads

		dWriteTicks := right.Value.WriteTicks - left.Value.WriteTicks
		dWrites := right.Value.Writes - left.Value.Writes

		// Guard against resets
		if dReadTicks >= 0 && dReads > 0 {
			// ReadTicks is in milliseconds
			avg := time.Duration(dReadTicks/int64(dReads)) * time.Millisecond
			readLatencies = append(readLatencies, avg)
		}

		if dWriteTicks >= 0 && dWrites > 0 {
			avg := time.Duration(dWriteTicks/int64(dWrites)) * time.Millisecond
			writeLatencies = append(writeLatencies, avg)
		}
	}

	if len(readLatencies) == 0 && len(writeLatencies) == 0 {
		return
	}

	s.updateHealth(readLatencies, writeLatencies)
}

func (s *LinuxBlockLayerStatisticsVolumeHealthSource) updateHealth(
	readLatencies, writeLatencies []time.Duration,
) {
	switch s.health {

	case domain.VolumeFailed:
		return

	case domain.VolumeDegraded:
		// Minimum degraded duration
		if time.Since(s.degradedAt) < 5*time.Second {
			return
		}

		// Last 5 samples
		if len(readLatencies) > 5 {
			readLatencies = readLatencies[len(readLatencies)-5:]
		}
		if len(writeLatencies) > 5 {
			writeLatencies = writeLatencies[len(writeLatencies)-5:]
		}

		// Recovery threshold
		for _, r := range readLatencies {
			if r > 80*time.Millisecond {
				return
			}
		}
		for _, w := range writeLatencies {
			if w > 80*time.Millisecond {
				return
			}
		}

		s.health = domain.VolumeOK
		return

	default:
		// OK → Degraded
		badReads := 0
		badWrites := 0

		for _, r := range readLatencies {
			if r > 100*time.Millisecond {
				badReads++
			}
		}
		for _, w := range writeLatencies {
			if w > 100*time.Millisecond {
				badWrites++
			}
		}

		if badReads >= 3 || badWrites >= 3 {
			s.health = domain.VolumeDegraded
			s.degradedAt = time.Now()
		}
	}
}

func (s *LinuxBlockLayerStatisticsVolumeHealthSource) Get() domain.VolumeHealth {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.health
}
