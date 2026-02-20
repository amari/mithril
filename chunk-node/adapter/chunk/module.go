package adapterchunk

import (
	"path/filepath"

	infrastructurepebble "github.com/amari/mithril/chunk-node/adapter/infrastructure/pebble"
	portchunk "github.com/amari/mithril/chunk-node/port/chunk"
	"github.com/cockroachdb/pebble/v2"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
)

func Module(dataDir string) fx.Option {
	return fx.Module("chunk",
		fx.Provide(NewChunkIDGenerator),
		fx.Provide(func(log *zerolog.Logger, lc fx.Lifecycle) (portchunk.ChunkRepository, error) {
			dbCfg := &infrastructurepebble.Config{
				Dir: filepath.Join(dataDir, "chunkDB"),
			}

			db, err := pebble.Open(dbCfg.Dir, dbCfg.PebbleOptions())
			if err != nil {
				return nil, err
			}

			lc.Append(fx.StopHook(db.Close))

			return NewPebbleChunkRepository(db), nil
		}),
	)
}
