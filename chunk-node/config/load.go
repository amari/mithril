package config

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	infrastructurekoanfproviderscliflagv3 "github.com/amari/mithril/chunk-node/adapter/infrastructure/koanf/providers/cliflagv3"
	"github.com/knadh/koanf/parsers/json"
	"github.com/knadh/koanf/parsers/toml"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
	"github.com/urfave/cli/v3"
)

// MergeFunc is a merge function that in a single pass:
//   - Overwrites scalar values
//   - Deep merges maps
//   - Appends arrays
func MergeFunc(src, dest map[string]any) error {
	visited := make(map[uintptr]struct{})
	merge(src, dest, visited)
	return nil
}

func merge(src, dest map[string]any, visited map[uintptr]struct{}) {
	// Guard against circular references
	srcPtr := reflect.ValueOf(src).Pointer()
	if _, seen := visited[srcPtr]; seen {
		return
	}
	visited[srcPtr] = struct{}{}
	for key, srcVal := range src {
		destVal, exists := dest[key]
		if !exists {
			dest[key] = srcVal
			continue
		}

		switch srcTyped := srcVal.(type) {
		case map[string]any:
			// Deep merge maps
			if destMap, ok := destVal.(map[string]any); ok {
				merge(srcTyped, destMap, visited)
				continue
			}
		case []any:
			// Append arrays
			if destSlice, ok := destVal.([]any); ok {
				dest[key] = append(destSlice, srcTyped...)
				continue
			}
		}

		// Overwrite scalars and mismatched types
		dest[key] = srcVal
	}
}

func LoadFile(k *koanf.Koanf, path string) error {
	var pa koanf.Parser

	ext := filepath.Ext(path)

	switch ext {
	case ".json":
		pa = json.Parser()
	case ".yaml", ".yml":
		pa = yaml.Parser()
	case ".toml":
		pa = toml.Parser()
	default:
		return fmt.Errorf("bad config file: %s", path)
	}

	return k.Load(file.Provider(path), pa, koanf.WithMergeFunc(MergeFunc))
}

func LoadDirectory(k *koanf.Koanf, dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("failed to read config directory: %w", err)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		path := filepath.Join(dir, entry.Name())
		if err := LoadFile(k, path); err != nil {
			return err
		}
	}

	return nil
}

func LoadEnv(k *koanf.Koanf, prefix string) error {
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

func LoadCLICommand(k *koanf.Koanf, cmd *cli.Command) error {
	delim := "."

	if err := k.Load(infrastructurekoanfproviderscliflagv3.Provider(cmd, delim, func(_ *cli.Command, flagName string) string {
		return strings.ReplaceAll(strings.ToLower(flagName), "-", delim)
	}), nil); err != nil {
		return err
	}

	return nil
}

func ExpandVars(k *koanf.Koanf, mapping func(string) string) error {
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

func ExpandEnvVars(k *koanf.Koanf) error {
	return ExpandVars(k, os.Getenv)
}
