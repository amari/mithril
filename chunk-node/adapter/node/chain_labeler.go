package node

import (
	"context"
	"maps"

	"github.com/amari/mithril/chunk-node/port"
	"github.com/rs/zerolog"
)

type chainNodeLabeler struct {
	Labelers []port.NodeLabeler
}

var _ port.NodeLabeler = (*chainNodeLabeler)(nil)

func NewChainNodeLabeler(labelers ...port.NodeLabeler) port.NodeLabeler {
	return &chainNodeLabeler{
		Labelers: labelers,
	}
}

func (cnl *chainNodeLabeler) Labels(ctx context.Context) (map[string]string, error) {
	result := make(map[string]string)
	for _, labeler := range cnl.Labelers {
		labels, err := labeler.Labels(ctx)
		if err != nil {
			zerolog.Ctx(ctx).Warn().Err(err).Msg("failed to get labels from labeler, skipping")

			continue
		}

		maps.Copy(result, labels)
	}

	return result, nil
}
