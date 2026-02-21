package config

import (
	"context"
	"fmt"
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/knadh/koanf/v2"
	"github.com/rs/zerolog"
)

type ClusterConfig struct {
	Cluster Cluster           `koanf:"cluster"`
	Etcd    ClusterConfigEtcd `koanf:"etcd"`
}

type Cluster string

const (
	ClusterTypeEtcd Cluster = "etcd"
)

func (t *Cluster) UnmarshalText(text []byte) error {
	switch string(text) {
	case string(ClusterTypeEtcd):
		*t = ClusterTypeEtcd
	default:
		return fmt.Errorf("unknown cluster: %s", string(text))
	}
	return nil
}

type ClusterConfigEtcd struct {
	Endpoints []string         `koanf:"endpoints"`
	TLS       *TLSClientConfig `koanf:"tls"`
}

// ClusterConfigProvider provides access to the cluster configuration and allows
// watching for configuration changes.
type ClusterConfigProvider interface {
	GetClusterConfig() (*ClusterConfig, error)
	Watch(ctx context.Context) <-chan struct{}
}

type ClusterDirectory struct {
	path string
	log  *zerolog.Logger

	mu     sync.RWMutex
	cached *ClusterConfig

	subscribersMu sync.Mutex
	subscribers   map[chan struct{}]struct{}

	ctx     context.Context
	cancel  context.CancelFunc
	watcher *fsnotify.Watcher
	wg      sync.WaitGroup
}

func NewClusterDirectory(path string, log *zerolog.Logger) *ClusterDirectory {
	return &ClusterDirectory{
		path:        path,
		log:         log,
		subscribers: make(map[chan struct{}]struct{}),
	}
}

func (d *ClusterDirectory) load() (*ClusterConfig, error) {
	k := koanf.New(".")

	if err := LoadDirectory(k, d.path); err != nil {
		return nil, err
	}

	var wrapper ClusterConfig
	if err := k.Unmarshal("", &wrapper); err != nil {
		return nil, err
	}

	return &wrapper, nil
}

func (d *ClusterDirectory) GetClusterConfig() (*ClusterConfig, error) {
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

func (d *ClusterDirectory) Watch(ctx context.Context) <-chan struct{} {
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

func (d *ClusterDirectory) notify() {
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

func (d *ClusterDirectory) run() {
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

			d.log.Debug().Str("path", event.Name).Str("op", event.Op.String()).Msg("cluster config changed")

			cfg, err := d.load()
			if err != nil {
				d.log.Error().Err(err).Msg("failed to reload cluster config")
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

func (d *ClusterDirectory) Start() error {
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

func (d *ClusterDirectory) Stop() error {
	if d.cancel != nil {
		d.cancel()
	}

	d.wg.Wait()

	if d.watcher != nil {
		return d.watcher.Close()
	}

	return nil
}

var _ ClusterConfigProvider = (*ClusterDirectory)(nil)
