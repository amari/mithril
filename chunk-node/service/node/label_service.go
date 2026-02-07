package node

import (
	"context"
	"fmt"
	"maps"

	"github.com/amari/mithril/chunk-node/port"
	"github.com/rs/zerolog"
)

type NodeLabelService struct {
	nodeLabeler        port.NodeLabeler
	nodeLabelPublisher port.NodeLabelPublisher
	log                *zerolog.Logger
}

func NewNodeLabelService(nodeLabeler port.NodeLabeler, nodeLabelPublisher port.NodeLabelPublisher, log *zerolog.Logger) *NodeLabelService {
	return &NodeLabelService{
		nodeLabeler:        nodeLabeler,
		nodeLabelPublisher: nodeLabelPublisher,
		log:                log,
	}
}

// CollectAndPublish aggregates labels from all sources and publishes to etcd
func (s *NodeLabelService) CollectAndPublish(ctx context.Context) error {
	labels := make(map[string]string)

	// Collect from labeler
	labelerLabels, err := s.nodeLabeler.Labels(ctx)
	if err != nil {
		s.log.Error().Err(err).Msg("failed to get labels from labeler")

		return fmt.Errorf("failed to get labels from labeler: %w", err)
	}
	maps.Copy(labels, labelerLabels)

	// Publish to etcd
	if err := s.nodeLabelPublisher.PublishLabels(ctx, labels); err != nil {
		return fmt.Errorf("failed to publish labels: %w", err)
	}

	return nil
}
