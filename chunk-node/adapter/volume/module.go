package adaptervolume

import (
	"path/filepath"

	infrastructurepebble "github.com/amari/mithril/chunk-node/adapter/infrastructure/pebble"
	"github.com/amari/mithril/chunk-node/adapter/volume/directory"
	"github.com/amari/mithril/chunk-node/adapter/volume/picker"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
	"github.com/cockroachdb/pebble/v2"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
)

func Module(dataDir string) fx.Option {
	return fx.Module("volume",
		directory.Module(),
		picker.Module(),

		fx.Provide(func(log *zerolog.Logger, lc fx.Lifecycle) (portvolume.VolumeIDAllocator, error) {
			dbCfg := &infrastructurepebble.Config{
				Dir: filepath.Join(dataDir, "volumeDB"),
			}

			db, err := pebble.Open(dbCfg.Dir, dbCfg.PebbleOptions())
			if err != nil {
				return nil, err
			}

			lc.Append(fx.StopHook(db.Close))

			return NewPebbleVolumeIDAllocator(db)
		}),
	)
}
