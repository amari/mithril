package adapterspebble

import (
	"path/filepath"

	"github.com/amari/mithril/mithril-node-go/internal/domain"
	"github.com/cockroachdb/pebble/v2"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/fx"
)

func Module(dataDir string) fx.Option {
	options := []fx.Option{
		fx.Provide(
			func(logger *zerolog.Logger, tp trace.TracerProvider, lc fx.Lifecycle) (domain.ChunkRepository, error) {
				dirname := filepath.Join(dataDir, "chunk-db")

				newLogger := logger.With().
					Str(string(semconv.DBSystemNameKey), "pebble").
					Str(string(semconv.DBNamespaceKey), dirname).
					Logger()

				db, err := pebble.Open(dirname, &pebble.Options{
					LoggerAndTracer: &LoggerAndTracer{
						Logger: &newLogger,
						Tracer: tp.Tracer("mithril-node-go/internal/adapters/pebble"),
						Attributes: []attribute.KeyValue{
							attribute.String("db.system", "pebble"),
							attribute.String("db.namespace", dirname),
						},
					},
				})
				if err != nil {
					// FIXME
					return nil, err
				}
				repo := NewChunkRepository(db)

				lc.Append(fx.StartStopHook(repo.Start, repo.Stop))

				return repo, nil
			},
			func(logger *zerolog.Logger, tp trace.TracerProvider, lc fx.Lifecycle) (domain.VolumeIDCounter, error) {
				dirname := filepath.Join(dataDir, "counter-db")

				newLogger := logger.With().
					Str(string(semconv.DBSystemNameKey), "pebble").
					Str(string(semconv.DBNamespaceKey), dirname).
					Logger()

				db, err := pebble.Open(dirname, &pebble.Options{
					LoggerAndTracer: &LoggerAndTracer{
						Logger: &newLogger,
						Tracer: tp.Tracer("mithril-node-go/internal/adapters/pebble"),
						Attributes: []attribute.KeyValue{
							attribute.String("db.system", "pebble"),
							attribute.String("db.namespace", dirname),
						},
					},
				})
				if err != nil {
					// FIXME
					return nil, err
				}
				repo := NewVolumeIDCounter(db)

				lc.Append(fx.StartHook(repo.Start))

				return repo, nil
			},
		),
	}

	return fx.Options(options...)
}
