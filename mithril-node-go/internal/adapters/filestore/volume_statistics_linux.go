//go:build linux
// +build linux

package adaptersfilestore

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	adapterssysfs "github.com/amari/mithril/mithril-node-go/internal/adapters/sysfs"
	"github.com/amari/mithril/mithril-node-go/internal/domain"
)

type IOKitIOBlockStorageDriverVolumeStatisticsProvider = NopVolumeStatisticsProvider[domain.IOKitIOBlockStorageDriverStatistics]

func NewIOKitIOBlockStorageDriverVolumeStatisticsProvider(path string, pollInterval time.Duration) (*IOKitIOBlockStorageDriverVolumeStatisticsProvider, error) {
	return NewNopVolumeStatisticsProvider[domain.IOKitIOBlockStorageDriverStatistics]()
}

type LinuxBlockLayerVolumeStatisticsProvider struct {
	path         string
	pollInterval time.Duration
	file         *os.File

	mu                    sync.RWMutex
	started               atomic.Bool
	pollContext           context.Context
	pollContextCancelFunc context.CancelFunc
	wg                    sync.WaitGroup
	subscribers           map[chan struct{}]struct{}
	sample                domain.Sample[domain.LinuxBlockLayerStatistics]
}

var _ domain.VolumeStatisticsProvider[domain.LinuxBlockLayerStatistics] = (*LinuxBlockLayerVolumeStatisticsProvider)(nil)

func NewLinuxBlockLayerVolumeStatisticsProvider(path string, pollInterval time.Duration) (*LinuxBlockLayerVolumeStatisticsProvider, error) {
	blockDevice, err := adapterssysfs.ResolveBlockDevice(path)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve block device: %w", err)
	}

	// Verify the stat file exists
	if _, err := os.Stat(blockDevice.StatPath()); err != nil {
		return nil, fmt.Errorf("failed to access sysfs stat file: %w", err)
	}

	return &LinuxBlockLayerVolumeStatisticsProvider{
		path:         blockDevice.StatPath(),
		pollInterval: pollInterval,

		subscribers: make(map[chan struct{}]struct{}),
		sample:      domain.Sample[domain.LinuxBlockLayerStatistics]{},
	}, nil
}

func (p *LinuxBlockLayerVolumeStatisticsProvider) start() error {
	if p.started.Load() {
		return nil
	}
	defer p.started.Store(true)

	p.mu.Lock()
	defer p.mu.Unlock()

	file, err := os.OpenFile(p.path, os.O_RDONLY, 0o444)
	if err != nil {
		return fmt.Errorf("failed to open sysfs stat file: %w", err)
	}

	p.file = file

	p.pollContext, p.pollContextCancelFunc = context.WithCancel(context.Background())

	p.wg.Go(func() {
		p.pollSync(p.pollContext)
	})

	return nil
}

func (p *LinuxBlockLayerVolumeStatisticsProvider) stop() error {
	if !p.started.Load() {
		return nil
	}
	defer p.started.Store(false)

	p.mu.Lock()
	defer p.mu.Unlock()

	p.pollContextCancelFunc()

	p.subscribers = nil

	p.wg.Wait()

	_ = p.file.Close()

	return nil
}

func (p *LinuxBlockLayerVolumeStatisticsProvider) pollSync(ctx context.Context) {
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

func (p *LinuxBlockLayerVolumeStatisticsProvider) collect(_ context.Context) {
	v, err := p.querySysfs()

	p.mu.Lock()
	defer p.mu.Unlock()

	sample := domain.Sample[domain.LinuxBlockLayerStatistics]{
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

func (p *LinuxBlockLayerVolumeStatisticsProvider) querySysfs() (domain.LinuxBlockLayerStatistics, error) {
	_, _ = p.file.Seek(0, io.SeekStart)

	scanner := bufio.NewScanner(p.file)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return domain.LinuxBlockLayerStatistics{}, fmt.Errorf("failed to read sysfs stat file: %w", err)
		}
		return domain.LinuxBlockLayerStatistics{}, fmt.Errorf("empty sysfs stat file")
	}

	fields := strings.Fields(scanner.Text())

	// The stat file has 17 fields (as of kernel 5.5+), but older kernels may have fewer.
	// Fields are space-separated integers in this order:
	//  0: read I/Os completed
	//  1: read I/Os merged
	//  2: sectors read
	//  3: time spent reading (ms)
	//  4: write I/Os completed
	//  5: write I/Os merged
	//  6: sectors written
	//  7: time spent writing (ms)
	//  8: I/Os currently in progress
	//  9: time spent doing I/Os (ms)
	// 10: weighted time spent doing I/Os (ms)
	// 11: discard I/Os completed (kernel 4.18+)
	// 12: discard I/Os merged
	// 13: sectors discarded
	// 14: time spent discarding (ms)
	// 15: flush I/Os completed (kernel 5.5+)
	// 16: time spent flushing (ms)

	return domain.LinuxBlockLayerStatistics{
		Reads:            parseInt64(fields, 0),
		MergedReads:      parseInt64(fields, 1),
		SectorsRead:      parseInt64(fields, 2),
		ReadTicks:        parseInt64(fields, 3),
		Writes:           parseInt64(fields, 4),
		MergedWrites:     parseInt64(fields, 5),
		SectorsWritten:   parseInt64(fields, 6),
		WriteTicks:       parseInt64(fields, 7),
		InFlight:         parseInt64(fields, 8),
		IOTicks:          parseInt64(fields, 9),
		TimeInQueue:      parseInt64(fields, 10),
		Discards:         parseInt64(fields, 11),
		MergedDiscards:   parseInt64(fields, 12),
		SectorsDiscarded: parseInt64(fields, 13),
		DiscardTicks:     parseInt64(fields, 14),
		Flushes:          parseInt64(fields, 15),
		FlushTicks:       parseInt64(fields, 16),
	}, nil
}

func parseInt64(fields []string, index int) int64 {
	if index >= len(fields) {
		return 0
	}
	v, _ := strconv.ParseInt(fields[index], 10, 64)
	return v
}

func (p *LinuxBlockLayerVolumeStatisticsProvider) Get() domain.Sample[domain.LinuxBlockLayerStatistics] {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.sample
}

func (p *LinuxBlockLayerVolumeStatisticsProvider) Watch(watchCtx context.Context) <-chan struct{} {
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
