package configpebble

import "github.com/cockroachdb/pebble/v2"

type DB struct {
	Dir string `koanf:"dir"`
	WAL *DBWAL `koanf:"wal"`
}

func (cfg *DB) PebbleOptions() *pebble.Options {
	opts := &pebble.Options{}

	if cfg.WAL != nil && cfg.WAL.Dir != "" {
		opts.WALDir = cfg.WAL.Dir
	}

	return opts
}

type DBWAL struct {
	Dir string `koanf:"dir"`
}
