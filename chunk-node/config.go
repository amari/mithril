package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	infrastructureetcd "github.com/amari/mithril/chunk-node/adapter/infrastructure/etcd"
	"github.com/amari/mithril/chunk-node/adapter/infrastructure/grpc"
	"github.com/amari/mithril/chunk-node/adapter/infrastructure/healthcheck"
	infrastructurekoanfproviderscliflagv3 "github.com/amari/mithril/chunk-node/adapter/infrastructure/koanf/providers/cliflagv3"
	"github.com/amari/mithril/chunk-node/adapter/infrastructure/log"
	"github.com/amari/mithril/chunk-node/adapter/infrastructure/pprof"
	adapternode "github.com/amari/mithril/chunk-node/adapter/node"
	"github.com/amari/mithril/chunk-node/service/volume"
	"github.com/knadh/koanf/parsers/json"
	"github.com/knadh/koanf/parsers/toml"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
	"github.com/urfave/cli/v3"
)

type Config struct {
	Etcd        infrastructureetcd.Config `koanf:"etcd"`
	Data        ConfigData                `koanf:"data"`
	GRPC        grpc.ServerConfig         `koanf:"grpc"`
	HealthCheck healthcheck.Config        `koanf:"healthCheck"`
	Log         log.Config                `koanf:"log"`
	Node        adapternode.Config        `koanf:"node"`
	Pprof       pprof.Config              `koanf:"pprof"`

	volume.Config `koanf:",squash"`
}

type ConfigData struct {
	Dir string `koanf:"dir"`
}

func LoadConfigFromFile(k *koanf.Koanf, name string) error {
	var pa koanf.Parser

	switch filepath.Ext(name) {
	case ".json":
		pa = json.Parser()
	case ".yaml", ".yml":
		pa = yaml.Parser()
	case ".toml":
		pa = toml.Parser()
	default:
		return fmt.Errorf("unsupported file extension: %s", filepath.Ext(name))
	}

	return k.Load(file.Provider(name), pa)
}

func LoadConfigFromDirectory(k *koanf.Koanf, dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		path := filepath.Join(dir, entry.Name())
		if err := LoadConfigFromFile(k, path); err != nil {
			return err
		}
	}

	return nil
}

func LoadConfigFromEnv(k *koanf.Koanf, prefix string) error {
	delim := "."

	return k.Load(env.ProviderWithValue(prefix, delim, func(k string, v string) (string, any) {
		key := strings.ReplaceAll(
			strings.ToLower(strings.TrimPrefix(k, prefix)),
			"_", delim,
		)
		if strings.Contains(v, ",") {
			return key, strings.Split(v, ",")
		}

		return key, v
	}), nil)
}

func LoadConfigFromCLI(k *koanf.Koanf, cmd *cli.Command) error {
	delim := "."

	if err := k.Load(infrastructurekoanfproviderscliflagv3.Provider(cmd, delim, func(_ *cli.Command, flagName string) string {
		return strings.ReplaceAll(strings.ToLower(flagName), "-", delim)
	}), nil); err != nil {
		return err
	}

	return nil
}

// expandKoanfValues replaces ${var} or $var in all string values in the koanf instanc based on the mapping function.
// For example, [expandKoanfValuesWithGetenv](k) is equivalent to [expandKoanfValues](k, [os.Getenv]).
func expandKoanfValues(k *koanf.Koanf, mapping func(string) string) error {
	for _, key := range k.Keys() {
		val := k.Get(key)
		strVal, ok := val.(string)
		if !ok {
			continue
		}

		// Simple expansion: look for $VAR and ${VAR}
		expandedVal := os.Expand(strVal, mapping)
		if expandedVal != val {
			k.Set(key, expandedVal)
		}
	}

	return nil
}

// expandKoanfValuesWithGetenv replaces ${var} or $var in all string values in the koanf instance according to the values of the current environment variables.
// References to undefined variables are replaced by the empty string.
func expandKoanfValuesWithGetenv(k *koanf.Koanf) error {
	return expandKoanfValues(k, os.Getenv)
}
