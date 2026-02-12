package volume

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/amari/mithril/chunk-node/port"
	"github.com/amari/mithril/chunk-node/port/volume"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/fx"
)

type Config struct {
	Volumes []*ConfigVolume `koanf:"volumes"`
}

type ConfigVolume struct {
	Type      VolumeType            `koanf:"type"`
	Directory ConfigVolumeDirectory `koanf:"directory"`
}

type ConfigVolumeDirectory struct {
	Path string `koanf:"path"`
}

func Module(directoryVolumePaths []string) fx.Option {
	return fx.Module("service.volume",
		fx.Provide(NewVolumeManager),
		fx.Provide(func(log *zerolog.Logger, lc fx.Lifecycle) (*VolumeStatsManager, portvolume.VolumeStatsProvider) {
			statsManager := NewVolumeStatsManager(log)

			lc.Append(fx.StopHook(statsManager.ClearVolumes))

			return statsManager, statsManager
		}),
		fx.Provide(
			func(statsManager *VolumeStatsManager,
				meter metric.Meter,
				log *zerolog.Logger,
				lc fx.Lifecycle,
			) (*VolumeHealthTracker, error) {
				healthTracker, err := NewVolumeHealthTracker(statsManager, meter, log, time.Now)
				if err != nil {
					return nil, err
				}
				lc.Append(fx.StopHook(healthTracker.ClearVolumes))

				return healthTracker, nil
			},
			func(healthTracker *VolumeHealthTracker) portvolume.VolumeHealthChecker {
				return healthTracker
			},
		),
		fx.Provide(func(nodeIdentityRepo port.NodeIdentityRepository, idAlloc volume.VolumeIDAllocator, directoryExpert volume.DirectoryVolumeExpert, volumeManager *VolumeManager, volumePicker volume.VolumePicker, healthTracker *VolumeHealthTracker, statsManager *VolumeStatsManager, log *zerolog.Logger, lc fx.Lifecycle) *VolumeService {
			svc := NewVolumeService(nodeIdentityRepo, idAlloc, directoryExpert, volumeManager, volumePicker, healthTracker, statsManager, log)

			lc.Append(fx.StopHook(svc.CloseAllVolumes))

			return svc
		}),
		fx.Provide(
			func(m *VolumeManager) portvolume.VolumeProvider {
				return m
			},
			func(svc *VolumeService) portvolume.VolumeCharacteristicsProvider {
				return svc
			},
			func(svc *VolumeService) portvolume.VolumeTelemetryProvider {
				return svc
			},
		),
		fx.Invoke(func(svc *VolumeService, log *zerolog.Logger, lc fx.Lifecycle) {
			lc.Append(fx.StartHook(func(ctx context.Context) error {
				for _, path := range directoryVolumePaths {
					log.Info().Str("path", path).Msg("Adding directory volume")

					if err := svc.AddDirectoryVolume(ctx, path, true); err != nil {
						return fmt.Errorf("failed to add directory volume (path: %s): %w", path, err)
					}
				}

				return nil
			}))
		}),
	)
}

type VolumeType string

const (
	VolumeTypeDirectory VolumeType = "directory"
)

var wellKnownVolumeTypes = map[string]VolumeType{
	"dir":       VolumeTypeDirectory,
	"directory": VolumeTypeDirectory,
}

func ParseVolumeType(s string) (VolumeType, error) {
	vt, ok := wellKnownVolumeTypes[strings.ToLower(s)]

	if !ok {
		return "", fmt.Errorf("unknown volume type: %s", s)
	}

	return vt, nil
}

func (vt VolumeType) String() string {
	return string(vt)
}

func (vt *VolumeType) UnmarshalText(text []byte) error {
	parsed, err := ParseVolumeType(string(text))
	if err != nil {
		return err
	}

	*vt = parsed

	return nil
}

func (vt VolumeType) MarshalText() ([]byte, error) {
	return []byte(vt.String()), nil
}

func (vt *VolumeType) UnmarshalJSON(data []byte) error {
	var s string

	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	parsed, err := ParseVolumeType(s)
	if err != nil {
		return err
	}

	*vt = parsed

	return nil
}

func (vt VolumeType) MarshalJSON() ([]byte, error) {
	return json.Marshal(vt.String())
}
