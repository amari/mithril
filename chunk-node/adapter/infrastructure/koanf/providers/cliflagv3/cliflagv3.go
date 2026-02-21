// Package cliflagv3 implements a koanf.Provider that reads commandline
// parameters as conf maps using urfave/cli/v3 flag.
package infrastructurekoanfproviderscliflagv3

import (
	"errors"

	"github.com/knadh/koanf/maps"
	"github.com/urfave/cli/v3"
)

// CliFlag implements a cli.Flag command line provider.
type CliFlag struct {
	cmd   *cli.Command
	delim string
	cb    func(cmd *cli.Command, key string, value any) (string, any)
}

// Provider returns a commandline flags provider that returns
// a nested map[string]any of environment variable where the
// nesting hierarchy of keys are defined by delim. For instance, the
// delim "." will convert the key `parent.child.key: 1`
// to `{parent: {child: {key: 1}}}`.
func Provider(cmd *cli.Command, delim string, cb func(cmd *cli.Command, s string) string) *CliFlag {
	c := &CliFlag{
		cmd:   cmd,
		delim: delim,
	}
	if cb != nil {
		c.cb = func(cmd *cli.Command, key string, value any) (string, any) {
			return cb(cmd, key), value
		}
	}

	return c
}

func ProviderWithValue(cmd *cli.Command, delim string, cb func(cmd *cli.Command, key string, value any) (string, interface{})) *CliFlag {
	return &CliFlag{
		cmd:   cmd,
		delim: delim,
		cb:    cb,
	}
}

// ReadBytes is not supported by the cliflagv3 provider.
func (c *CliFlag) ReadBytes() ([]byte, error) {
	return nil, errors.New("cliflagv3 provider does not support this method")
}

// Watch is not supported.
func (c *CliFlag) Watch(cb func(event any, err error)) error {
	return errors.New("cliflagv3 provider does not support this method")
}

// Read reads the flag variables and returns a nested conf map.
func (c *CliFlag) Read() (map[string]any, error) {
	// Get command lineage (command to root)
	lineage := c.cmd.Lineage()
	// Reverse lineage to get root to command order
	for i, j := 0, len(lineage)-1; i < j; i, j = i+1, j-1 {
		lineage[i], lineage[j] = lineage[j], lineage[i]
	}

	mp := make(map[string]any)
	for _, cmd := range lineage {
		for _, flag := range cmd.Flags {
			if !flag.IsSet() {
				continue
			}

			flagValue := flag.Get()

			for _, flagName := range flag.Names() {
				if c.cb != nil {
					key, value := c.cb(cmd, flagName, flagValue)
					mp[key] = value
				} else {
					mp[flagName] = flagValue
				}
			}
		}
	}

	if c.delim != "" {
		return maps.Unflatten(mp, c.delim), nil
	}

	return mp, nil
}
