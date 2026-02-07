package pebble

import (
	"fmt"

	"github.com/cockroachdb/pebble/v2"
)

type Config struct {
	Dir    string `koanf:"dir"`
	WALDir string `koanf:"walDir"`
}

// Validate validates the Pebble configuration
func (cfg *Config) Validate() error {
	if cfg.Dir == "" {
		return fmt.Errorf("pebble dir is required")
	}
	return nil
}

// PebbleOptions returns the Pebble DB options
func (cfg *Config) PebbleOptions() *pebble.Options {
	opts := &pebble.Options{}

	if cfg.WALDir != "" {
		opts.WALDir = cfg.WALDir
	}

	return opts
}
