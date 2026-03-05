//go:build darwin
// +build darwin

package adaptersfilestore

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/amari/mithril/mithril-node-go/internal/adapters/iokit"
	"github.com/amari/mithril/mithril-node-go/internal/domain"
)

type LinuxBlockLayerVolumeStatisticsProvider = NopVolumeStatisticsProvider[domain.LinuxBlockLayerStatistics]

func NewLinuxBlockLayerVolumeStatisticsProvider(path string, pollInterval time.Duration) (*LinuxBlockLayerVolumeStatisticsProvider, error) {
	return NewNopVolumeStatisticsProvider[domain.LinuxBlockLayerStatistics]()
}

type IOKitIOBlockStorageDriverVolumeStatisticsProvider struct {
	path         string
	driver       *iokit.IOBlockStorageDriver
	pollInterval time.Duration

	mu                    sync.RWMutex
	started               atomic.Bool
	pollContext           context.Context
	pollContextCancelFunc context.CancelFunc
	wg                    sync.WaitGroup
	subscribers           map[chan struct{}]struct{}
	sample                domain.Sample[domain.IOKitIOBlockStorageDriverStatistics]
}

var _ domain.VolumeStatisticsProvider[domain.IOKitIOBlockStorageDriverStatistics] = (*IOKitIOBlockStorageDriverVolumeStatisticsProvider)(nil)

func NewIOKitIOBlockStorageDriverVolumeStatisticsProvider(path string, pollInterval time.Duration) (*IOKitIOBlockStorageDriverVolumeStatisticsProvider, error) {
	return &IOKitIOBlockStorageDriverVolumeStatisticsProvider{
		path:         path,
		pollInterval: pollInterval,

		subscribers: make(map[chan struct{}]struct{}),
		sample:      domain.Sample[domain.IOKitIOBlockStorageDriverStatistics]{},
	}, nil
}

func (p *IOKitIOBlockStorageDriverVolumeStatisticsProvider) start() error {
	if p.started.Load() {
		return nil
	}
	defer p.started.Store(true)

	p.mu.Lock()
	defer p.mu.Unlock()

	media, err := iokit.IOMediaFromPath(p.path)
	if err != nil {
		return fmt.Errorf("failed to get IOMedia from path: %w", err)
	}
	defer media.Close()

	driver, err := iokit.GetBlockStorageDriverFromMedia(media)
	if err != nil {
		return fmt.Errorf("failed to get IOBlockStorageDriver: %w", err)
	}

	p.driver = driver

	p.pollContext, p.pollContextCancelFunc = context.WithCancel(context.Background())

	p.wg.Go(func() {
		p.pollSync(p.pollContext)
	})

	return nil
}

func (p *IOKitIOBlockStorageDriverVolumeStatisticsProvider) stop() error {
	if !p.started.Load() {
		return nil
	}
	defer p.started.Store(false)

	p.mu.Lock()
	defer p.mu.Unlock()

	p.pollContextCancelFunc()

	p.subscribers = nil

	p.wg.Wait()

	_ = p.driver.Close()

	return nil
}

func (p *IOKitIOBlockStorageDriverVolumeStatisticsProvider) pollSync(ctx context.Context) {
	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.collect(ctx)
		}
	}
}

func (p *IOKitIOBlockStorageDriverVolumeStatisticsProvider) collect(_ context.Context) {
	v, err := p.queryIOKit()

	p.mu.Lock()
	defer p.mu.Unlock()

	sample := domain.Sample[domain.IOKitIOBlockStorageDriverStatistics]{
		Value: p.sample.Value,
		Epoch: p.sample.Epoch + 1,
		Time:  p.sample.Time,
	}
	if err != nil {
		sample.Error = err

		return
	}

	sample.Value = v
	sample.Time = time.Now()

	p.sample = sample

	for ch := range p.subscribers {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (p *IOKitIOBlockStorageDriverVolumeStatisticsProvider) queryIOKit() (domain.IOKitIOBlockStorageDriverStatistics, error) {
	// Read raw statistics dictionary from IOKit
	rawStats, err := p.driver.ReadRawStatistics()
	if err != nil {
		return domain.IOKitIOBlockStorageDriverStatistics{}, fmt.Errorf("failed to read IOKit statistics: %w", err)
	}

	return domain.IOKitIOBlockStorageDriverStatistics{
		BytesRead:       getInt64(rawStats, "Bytes (Read)"),
		BytesWritten:    getInt64(rawStats, "Bytes (Write)"),
		LatentReadTime:  getInt64(rawStats, "Latency Time (Read)"),
		LatentWriteTime: getInt64(rawStats, "Latency Time (Write)"),
		ReadErrors:      getInt64(rawStats, "Errors (Read)"),
		ReadRetries:     getInt64(rawStats, "Retries (Read)"),
		Reads:           getInt64(rawStats, "Operations (Read)"),
		TotalReadTime:   getInt64(rawStats, "Total Time (Read)"),
		TotalWriteTime:  getInt64(rawStats, "Total Time (Write)"),
		WriteErrors:     getInt64(rawStats, "Errors (Write)"),
		WriteRetries:    getInt64(rawStats, "Retries (Write)"),
		Writes:          getInt64(rawStats, "Operations (Write)"),
	}, nil
}

func getInt64(m map[string]any, key string) int64 {
	if v, ok := m[key].(int64); ok {
		return v
	}
	return 0
}

func (p *IOKitIOBlockStorageDriverVolumeStatisticsProvider) Get() domain.Sample[domain.IOKitIOBlockStorageDriverStatistics] {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.sample
}

func (p *IOKitIOBlockStorageDriverVolumeStatisticsProvider) Watch(watchCtx context.Context) <-chan struct{} {
	if !p.started.Load() {
		ch := make(chan struct{})
		close(ch)
		return ch
	}

	ch := make(chan struct{}, 1)

	p.mu.Lock()
	defer p.mu.Unlock()

	go func() {
		defer close(ch)

		select {
		case <-watchCtx.Done():
		case <-p.pollContext.Done():
		}

		p.mu.Lock()
		delete(p.subscribers, ch)
		p.mu.Unlock()
	}()

	p.subscribers[ch] = struct{}{}

	return ch
}
