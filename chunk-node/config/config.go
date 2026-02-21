package config

import (
	"github.com/amari/mithril/chunk-node/adapter/infrastructure/log"
)

type Config struct {
	Config ConfigConfig `koanf:"config"`
	Data   ConfigData   `koanf:"data"`
	Log    log.Config   `koanf:"log"`
}

type ConfigConfig struct {
	DirPath string `koanf:"dir"`
}

type ConfigData struct {
	DirPath string `koanf:"dir"`
}
