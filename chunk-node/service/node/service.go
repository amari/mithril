package node

import (
	"context"
	"maps"

	adapternodelabeler "github.com/amari/mithril/chunk-node/adapter/node/labelcollector"
	portnode "github.com/amari/mithril/chunk-node/port/node"
	"github.com/rs/zerolog"
)

type LabelService struct {
	labelPublisher               portnode.NodeLabelPublisher
	runtimeLabelCollector        portnode.NodeLabelCollector
	kubernetesPodLabelCollector  portnode.NodeLabelCollector
	kubernetesNodeLabelCollector portnode.NodeLabelCollector
	log                          *zerolog.Logger
}

func NewLabelService(
	labelPublisher portnode.NodeLabelPublisher,
	runtimeLabelCollector *adapternodelabeler.Runtime,
	kubernetesPodLabelCollector *adapternodelabeler.KubernetesPod,
	kubernetesNodeLabelCollector *adapternodelabeler.KubernetesNode,
	log *zerolog.Logger,
) *LabelService {
	return &LabelService{
		labelPublisher:        labelPublisher,
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
		log: log,
	}
}

func (s *LabelService) Start(ctx context.Context) error {
	// TODO: watch each collector

	return nil
}

func (s *LabelService) Stop(ctx context.Context) error {
	return nil
}

func (s *LabelService) publishLabels(ctx context.Context) error {
	labels := make(map[string]string)

	// Collect from runtime
	runtimeLabels, err := s.runtimeLabelCollector.CollectNodeLabels(ctx)
	if err != nil {
		s.log.Error().Err(err).Msg("failed to collect runtime labels")
	} else {
		maps.Copy(labels, runtimeLabels)
	}

	// Collect from Kubernetes pods
	podLabels, err := s.kubernetesPodLabelCollector.CollectNodeLabels(ctx)
	if err != nil {
		s.log.Error().Err(err).Msg("failed to collect Kubernetes pod labels")
	} else {
		maps.Copy(labels, podLabels)
	}

	// Collect from Kubernetes node
	nodeLabels, err := s.kubernetesNodeLabelCollector.CollectNodeLabels(ctx)
	if err != nil {
		s.log.Error().Err(err).Msg("failed to collect Kubernetes node labels")
	} else {
		maps.Copy(labels, nodeLabels)
	}

	return s.labelPublisher.PublishNodeLabels(labels)
}
