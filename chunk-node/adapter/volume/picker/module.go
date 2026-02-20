package picker

import (
	"encoding/json"
	"fmt"
	"strings"

	portvolume "github.com/amari/mithril/chunk-node/port/volume"
	"go.uber.org/fx"
)

type Config struct {
	Type VolumePickerType `koanf:"type"`
}

func Module() fx.Option {
	return fx.Module("volume.picker",
		fx.Provide(func() portvolume.VolumePicker {
			return &RoundRobin{}
		}),
	)
}

type VolumePickerType string

const (
	VolumePickerTypePowerOfTwo VolumePickerType = "pow2"
	VolumePickerTypeRandom     VolumePickerType = "random"
	VolumePickerTypeRoundRobin VolumePickerType = "rr"
)

var wellKnownPickerTypes = map[string]VolumePickerType{
	"pow2":       VolumePickerTypePowerOfTwo,
	"poweroftwo": VolumePickerTypePowerOfTwo,
	"rand":       VolumePickerTypeRandom,
	"random":     VolumePickerTypeRandom,
	"rr":         VolumePickerTypeRoundRobin,
	"roundrobin": VolumePickerTypeRoundRobin,
}

func ParseVolumePickerType(s string) (VolumePickerType, error) {
	t, ok := wellKnownPickerTypes[strings.ToLower(s)]
	if !ok {
		return "", fmt.Errorf("unknown volume picker type: %s", s)
	}

	return t, nil
}

func (v VolumePickerType) String() string {
	return string(v)
}

func (v *VolumePickerType) UnmarshalText(text []byte) error {
	parsed, err := ParseVolumePickerType(string(text))
	if err != nil {
		return err
	}

	*v = parsed

	return nil
}

func (v *VolumePickerType) MarshalText() ([]byte, error) {
	return []byte(v.String()), nil
}

func (v *VolumePickerType) UnmarshalJSON(data []byte) error {
	var s string

	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	parsed, err := ParseVolumePickerType(s)
	if err != nil {
		return err
	}

	*v = parsed

	return nil
}

func (v *VolumePickerType) MarshalJSON() ([]byte, error) {
	return json.Marshal(v.String())
}
