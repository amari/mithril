package adaptersfilesystem

import (
	"path/filepath"

	"github.com/amari/mithril/mithril-node-go/internal/domain"
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
		),
	}

	return fx.Options(options...)
}
