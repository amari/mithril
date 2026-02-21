package config

import (
	"context"
	"fmt"
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/knadh/koanf/v2"
	"github.com/rs/zerolog"
)

type NodeConfig struct {
	Advertise NodeAdvertiseConfig `koanf:"advertise"`
	GRPC      NodeGRPCConfig      `koanf:"grpc"`
	Health    NodeHealthConfig    `koanf:"health"`
	Labels    map[string]string   `koanf:"labels"`
	Peer      NodePeerConfig      `koanf:"peer"`
	PProf     NodePProfConfig     `koanf:"pprof"`
}

type NodeAdvertiseConfig struct {
	GRPC NodeAdvertiseGRPCConfig `koanf:"grpc"`
}

type NodeAdvertiseGRPCConfig struct {
	URLs []string `koanf:"urls"`
}

type NodeGRPCConfig struct {
	Enable bool             `koanf:"enable" default:"true"`
	Listen string           `koanf:"listen" default:"[::]:50051"`
	TLS    *TLSServerConfig `koanf:"tls"`
}

type NodeHealthConfig struct {
	Enable bool   `koanf:"enable" default:"true"`
	Listen string `koanf:"listen" default:"[::]:8080"`
}

type NodePeerConfig struct {
	TLS *TLSClientConfig `koanf:"tls"`
}

type NodePProfConfig struct {
	Enable bool   `koanf:"enable" default:"false"`
	Listen string `koanf:"listen" default:"[::]:6060"`
}

// NodeConfigProvider provides access to the node configuration and allows
// watching for configuration changes.
type NodeConfigProvider interface {
	GetNodeConfig() (*NodeConfig, error)
	Watch(ctx context.Context) <-chan struct{}
}

type NodeDirectory struct {
	path string
	log  *zerolog.Logger

	mu     sync.RWMutex
	cached *NodeConfig

	subscribersMu sync.Mutex
	subscribers   map[chan struct{}]struct{}

	ctx     context.Context
	cancel  context.CancelFunc
	watcher *fsnotify.Watcher
	wg      sync.WaitGroup
}

func NewNodeDirectory(path string, log *zerolog.Logger) *NodeDirectory {
	return &NodeDirectory{
		path:        path,
		log:         log,
		subscribers: make(map[chan struct{}]struct{}),
	}
}

func (d *NodeDirectory) load() (*NodeConfig, error) {
	k := koanf.New(".")

	if err := LoadDirectory(k, d.path); err != nil {
		return nil, err
	}

	var wrapper NodeConfig
	if err := k.Unmarshal("", &wrapper); err != nil {
		return nil, err
	}

	return &wrapper, nil
}

func (d *NodeDirectory) GetNodeConfig() (*NodeConfig, error) {
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

func (d *NodeDirectory) Watch(ctx context.Context) <-chan struct{} {
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

func (d *NodeDirectory) notify() {
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

func (d *NodeDirectory) run() {
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

			d.log.Debug().Str("path", event.Name).Str("op", event.Op.String()).Msg("node config changed")

			cfg, err := d.load()
			if err != nil {
				d.log.Error().Err(err).Msg("failed to reload node config")
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

func (d *NodeDirectory) Start() error {
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

func (d *NodeDirectory) Stop() error {
	if d.cancel != nil {
		d.cancel()
	}

	d.wg.Wait()

	if d.watcher != nil {
		return d.watcher.Close()
	}

	return nil
}

var _ NodeConfigProvider = (*NodeDirectory)(nil)
