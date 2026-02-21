package config

import (
	"context"
	"fmt"
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/knadh/koanf/v2"
	"github.com/rs/zerolog"
)

type VolumesDirectory struct {
	path string
	log  *zerolog.Logger

	mu     sync.RWMutex
	cached *VolumesConfig

	subscribersMu sync.Mutex
	subscribers   map[chan struct{}]struct{}

	ctx     context.Context
	cancel  context.CancelFunc
	watcher *fsnotify.Watcher
	wg      sync.WaitGroup
}

func NewVolumesDirectory(path string, log *zerolog.Logger) *VolumesDirectory {
	return &VolumesDirectory{
		path:        path,
		log:         log,
		subscribers: make(map[chan struct{}]struct{}),
	}
}

func (d *VolumesDirectory) load() (*VolumesConfig, error) {
	k := koanf.New(".")

	if err := LoadDirectory(k, d.path); err != nil {
		return nil, err
	}

	var cfg VolumesConfig
	if err := k.Unmarshal("", &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (d *VolumesDirectory) GetVolumesConfig() (*VolumesConfig, error) {
	d.mu.RLock()
	if d.cached != nil {
		defer d.mu.RUnlock()
		return d.cached, nil
	}
	d.mu.RUnlock()

	d.mu.Lock()
	defer d.mu.Unlock()

	// Double-check after acquiring write lock
	if d.cached != nil {
		return d.cached, nil
	}

	cfg, err := d.load()
	if err != nil {
		return nil, err
	}

	d.cached = cfg
	return d.cached, nil
}

func (d *VolumesDirectory) Watch(ctx context.Context) <-chan struct{} {
	ch := make(chan struct{}, 1)

	d.subscribersMu.Lock()
	d.subscribers[ch] = struct{}{}
	d.subscribersMu.Unlock()

	go func() {
		<-ctx.Done()

		d.subscribersMu.Lock()
		delete(d.subscribers, ch)
		close(ch)
		d.subscribersMu.Unlock()
	}()

	return ch
}

func (d *VolumesDirectory) notify() {
	d.subscribersMu.Lock()
	defer d.subscribersMu.Unlock()

	for ch := range d.subscribers {
		// Non-blocking send
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (d *VolumesDirectory) run() {
	defer d.wg.Done()

	for {
		select {
		case <-d.ctx.Done():
			return
		case event, ok := <-d.watcher.Events:
			if !ok {
				return
			}

			if event.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Remove|fsnotify.Rename) == 0 {
				continue
			}

			d.log.Debug().Str("path", event.Name).Str("op", event.Op.String()).Msg("volumes config changed")

			cfg, err := d.load()
			if err != nil {
				d.log.Error().Err(err).Msg("failed to reload volumes config")
				continue
			}

			d.mu.Lock()
			d.cached = cfg
			d.mu.Unlock()

			d.notify()

		case err, ok := <-d.watcher.Errors:
			if !ok {
				return
			}
			d.log.Error().Err(err).Msg("fsnotify error")
		}
	}
}

func (d *VolumesDirectory) Start() error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("failed to create fsnotify watcher: %w", err)
	}

	if err := watcher.Add(d.path); err != nil {
		watcher.Close()
		return fmt.Errorf("failed to watch directory %s: %w", d.path, err)
	}

	d.watcher = watcher
	d.ctx, d.cancel = context.WithCancel(context.Background())

	d.wg.Add(1)
	go d.run()

	return nil
}

func (d *VolumesDirectory) Stop() error {
	if d.cancel != nil {
		d.cancel()
	}

	d.wg.Wait()

	if d.watcher != nil {
		return d.watcher.Close()
	}

	return nil
}

var _ VolumesConfigProvider = (*VolumesDirectory)(nil)
