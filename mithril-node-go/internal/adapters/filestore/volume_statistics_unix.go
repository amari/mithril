package adaptersfilestore

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/amari/mithril/mithril-node-go/internal/domain"
	"golang.org/x/sys/unix"
)

type SpaceUtilizationVolumeStatisticsProvider struct {
	file         *os.File
	pollInterval time.Duration

	mu                    sync.RWMutex
	started               atomic.Bool
	pollContext           context.Context
	pollContextCancelFunc context.CancelFunc
	wg                    sync.WaitGroup
	subscribers           map[chan struct{}]struct{}
	sample                domain.Sample[domain.SpaceUtilizationStatistics]
}

var _ domain.VolumeStatisticsProvider[domain.SpaceUtilizationStatistics] = (*SpaceUtilizationVolumeStatisticsProvider)(nil)

func NewSpaceUtilizationVolumeStatisticsProvider(path string, pollInterval time.Duration) (*SpaceUtilizationVolumeStatisticsProvider, error) {
	file, err := os.OpenFile(path, unix.O_DIRECTORY|unix.O_RDONLY, 0o555)
	if err != nil {
		return nil, err
	}

	return &SpaceUtilizationVolumeStatisticsProvider{
		file:         file,
		pollInterval: pollInterval,

		subscribers: make(map[chan struct{}]struct{}),
		sample:      domain.Sample[domain.SpaceUtilizationStatistics]{},
	}, nil
}

func (p *SpaceUtilizationVolumeStatisticsProvider) start() error {
	if p.started.Load() {
		return nil
	}

	p.pollContext, p.pollContextCancelFunc = context.WithCancel(context.Background())

	p.wg.Go(func() {
		p.pollSync(p.pollContext)
	})

	return nil
}

func (p *SpaceUtilizationVolumeStatisticsProvider) stop() error {
	if !p.started.Load() {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.pollContextCancelFunc()

	p.subscribers = nil

	p.wg.Wait()

	return nil
}

func (p *SpaceUtilizationVolumeStatisticsProvider) pollSync(ctx context.Context) {
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

func (p *SpaceUtilizationVolumeStatisticsProvider) collect(_ context.Context) {
	v, err := p.queryStatfs()

	p.mu.Lock()
	defer p.mu.Unlock()

	sample := domain.Sample[domain.SpaceUtilizationStatistics]{
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

func (p *SpaceUtilizationVolumeStatisticsProvider) queryStatfs() (domain.SpaceUtilizationStatistics, error) {
	var stat unix.Statfs_t
	if err := unix.Fstatfs(int(p.file.Fd()), &stat); err != nil {
		return domain.SpaceUtilizationStatistics{}, fmt.Errorf("failed to statfs: %w", err)
	}

	return domain.SpaceUtilizationStatistics{
		TotalBytes: int64(stat.Blocks) * int64(stat.Bsize),
		FreeBytes:  int64(stat.Bavail) * int64(stat.Bsize),
		UsedBytes:  int64(stat.Blocks-stat.Bfree) * int64(stat.Bsize),
	}, nil
}

func (p *SpaceUtilizationVolumeStatisticsProvider) Get() domain.Sample[domain.SpaceUtilizationStatistics] {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.sample
}

func (p *SpaceUtilizationVolumeStatisticsProvider) Watch(watchCtx context.Context) <-chan struct{} {
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
