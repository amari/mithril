package adaptersfilesystem

import (
	"path/filepath"
	"time"

	applicationservices "github.com/amari/mithril/mithril-node-go/internal/application/services"
	"github.com/amari/mithril/mithril-node-go/internal/domain"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
)

func Module(dataDir string) fx.Option {
	options := []fx.Option{
		fx.Provide(
			func(lc fx.Lifecycle) (domain.NodeClaimRepository, domain.NodeIDProvider, error) {
				path := filepath.Join(dataDir, "node-claim")

				repo := NewNodeClaimRepository(path)

				lc.Append(fx.StartHook(repo.Start))

				return repo, repo, nil
			},
			func(lc fx.Lifecycle) (domain.NodeSeedRepository, error) {
				path := filepath.Join(dataDir, "node-seed")

				repo := NewNodeSeedRepository(path)

				lc.Append(fx.StartHook(repo.Start))

				return repo, nil
			},
			func(logger *zerolog.Logger, lc fx.Lifecycle) (applicationservices.ClockFence, error) {
				path := filepath.Join(dataDir, "clock-fence")
				fence := NewWallClockFenceFile(logger, path, 30*time.Second)

				lc.Append(fx.StartStopHook(fence.start, fence.stop))

				return fence, nil
			},
		),
	}

	return fx.Options(options...)
}
