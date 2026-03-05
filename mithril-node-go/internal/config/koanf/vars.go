package configkoanf

import (
	"os"

	"github.com/knadh/koanf/v2"
)

func ExpandEnvVars(k *koanf.Koanf) error {
	return ExpandVars(k, os.Getenv)
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
