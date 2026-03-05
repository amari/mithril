package adaptersfilestore

import (
	"github.com/amari/mithril/mithril-node-go/internal/domain"
	"go.uber.org/fx"
)

func Module() fx.Option {
	return fx.Provide(
		fx.Annotate(
			NewFileStoreFormat,
			fx.As(new(domain.FileStoreFormat)),
		),
	)
}
