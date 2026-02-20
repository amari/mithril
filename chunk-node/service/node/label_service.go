package node

import (
	"context"
	"fmt"
	"maps"

	adapternodelabeler "github.com/amari/mithril/chunk-node/adapter/node/labelcollector"
	"github.com/amari/mithril/chunk-node/port"
	portnode "github.com/amari/mithril/chunk-node/port/node"
	"github.com/rs/zerolog"
)

type NodeLabelService struct {
	nodeLabeler        port.NodeLabeler
	nodeLabelPublisher portnode.NodeLabelPublisher
	log                *zerolog.Logger

	runtimeLabelCollector        portnode.NodeLabelCollector
	kubernetesPodLabelCollector  portnode.NodeLabelCollector
	kubernetesNodeLabelCollector portnode.NodeLabelCollector
}

func NewNodeLabelService(
	nodeLabelPublisher portnode.NodeLabelPublisher,
	log *zerolog.Logger,
	runtimeLabelCollector *adapternodelabeler.Runtime,
	kubernetesPodLabelCollector *adapternodelabeler.KubernetesPod,
	kubernetesNodeLabelCollector *adapternodelabeler.KubernetesNode,
) *NodeLabelService {
	return &NodeLabelService{
		nodeLabelPublisher: nodeLabelPublisher,
		log:                log,

		runtimeLabelCollector: runtimeLabelCollector,
		kubernetesPodLabelCollector: adapternodelabeler.First(
			adapternodelabeler.Backoff(
				kubernetesPodLabelCollector,
				adapternodelabeler.BackoffOptions{},
			),
		),
		kubernetesNodeLabelCollector: adapternodelabeler.Debounce(
			adapternodelabeler.Backoff(
				kubernetesNodeLabelCollector,
				adapternodelabeler.BackoffOptions{},
			),
			adapternodelabeler.DebounceOptions{},
		),
	}
}

// CollectAndPublish aggregates labels from all sources and publishes to etcd
func (s *NodeLabelService) CollectAndPublish(ctx context.Context) error {
	labels := make(map[string]string)

	// Collect from runtime
	runtimeLabels, err := s.runtimeLabelCollector.CollectNodeLabels(ctx)
	if err != nil {
		s.log.Error().Err(err).Msg("failed to collect runtime labels")

		return fmt.Errorf("failed to collect runtime labels: %w", err)
	}
	maps.Copy(labels, runtimeLabels)

	if s.kubernetesNodeLabelCollector != nil {
		// Collect from Kubernetes node
		kubeNodeLabels, err := s.kubernetesNodeLabelCollector.CollectNodeLabels(ctx)
		if err != nil {
			s.log.Error().Err(err).Msg("failed to collect Kubernetes node labels")

			return fmt.Errorf("failed to collect Kubernetes node labels: %w", err)
		}
		maps.Copy(labels, kubeNodeLabels)
	}

	if s.kubernetesPodLabelCollector != nil {
		// Collect from Kubernetes pod
		kubePodLabels, err := s.kubernetesPodLabelCollector.CollectNodeLabels(ctx)
		if err != nil {
			s.log.Error().Err(err).Msg("failed to collect Kubernetes pod labels")

			return fmt.Errorf("failed to collect Kubernetes pod labels: %w", err)
		}
		maps.Copy(labels, kubePodLabels)
	}

	// Publish to etcd
	if err := s.nodeLabelPublisher.PublishNodeLabels(labels); err != nil {
		return fmt.Errorf("failed to publish labels: %w", err)
	}

	return nil
}
