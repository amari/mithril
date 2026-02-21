package config

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
)

// AbsolutePath is a filesystem path that is normalized to an absolute path
// during unmarshaling.
type AbsolutePath string

func (p *AbsolutePath) UnmarshalText(text []byte) error {
	path := string(text)
	abs, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("invalid path %q: %w", path, err)
	}
	*p = AbsolutePath(filepath.Clean(abs))
	return nil
}

func (p AbsolutePath) String() string {
	return string(p)
}

type VolumesConfig struct {
	Volumes []VolumeConfig `koanf:"volumes"`
}

type VolumeConfig struct {
	MatchPaths []AbsolutePath    `koanf:"matchPaths"`
	Labels     map[string]string `koanf:"labels"`
}

type VolumesConfigProvider interface {
	GetVolumesConfig() (*VolumesConfig, error)
	Watch(watchCtx context.Context) <-chan struct{}
}

// MatchPath returns the first VolumeConfig whose MatchPaths contains the given path,
// or nil if no match is found. The path is normalized to an absolute path before matching.
func (c *VolumesConfig) MatchPath(path string) *VolumeConfig {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil
	}
	abs = filepath.Clean(abs)
	for i := range c.Volumes {
		if slices.Contains(c.Volumes[i].MatchPaths, AbsolutePath(abs)) {
			return &c.Volumes[i]
		}
	}
	return nil
}

// WatchVolumeConfigForPath returns a channel that emits the VolumeConfig matching
// the given path whenever the configuration changes. It sends the initial matching
// config immediately (if any), then sends updates on each config reload.
//
// The channel emits nil if the path no longer matches any VolumeConfig.
// The channel is closed when ctx is cancelled.
func WatchVolumeConfigForPath(ctx context.Context, provider VolumesConfigProvider, path string) <-chan *VolumeConfig {
	ch := make(chan *VolumeConfig, 1)

	go func() {
		defer close(ch)

		// Send initial config
		cfg, err := provider.GetVolumesConfig()
		if err == nil {
			if match := cfg.MatchPath(path); match != nil {
				ch <- match
			}
		}

		watchCh := provider.Watch(ctx)
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-watchCh:
				if !ok {
					return
				}

				cfg, err := provider.GetVolumesConfig()
				if err != nil {
					continue
				}

				match := cfg.MatchPath(path)
				// Non-blocking send
				select {
				case ch <- match:
				default:
				}
			}
		}
	}()

	return ch
}
