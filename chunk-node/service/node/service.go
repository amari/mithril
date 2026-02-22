package node

import (
	"context"
	"maps"
	"sync"

	portnode "github.com/amari/mithril/chunk-node/port/node"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
)

type LabelService struct {
	labelPublisher portnode.NodeLabelPublisher
	log            *zerolog.Logger

	labelProviders []portnode.NodeLabelProvider

	watchCtx        context.Context
	watchCancelFunc context.CancelFunc
	wg              sync.WaitGroup
}

type LabelServiceParams struct {
	fx.In

	LabelPublisher portnode.NodeLabelPublisher
	LabelProviders []portnode.NodeLabelProvider `group:"node-label-provider"`
	Logger         *zerolog.Logger
}

func NewLabelService(
	params LabelServiceParams,
) *LabelService {
	return &LabelService{
		labelProviders: params.LabelProviders,
		labelPublisher: params.LabelPublisher,
		log:            params.Logger,
	}
}

func (s *LabelService) Start(ctx context.Context) error {
	watchCtx, cancelF := context.WithCancel(context.Background())

	s.watchCtx = watchCtx
	s.watchCancelFunc = cancelF

	for _, provider := range s.labelProviders {
		if provider == nil {
			continue
		}

		s.wg.Go(func() {
			ch := provider.Watch(watchCtx)

			select {
			case <-ch:
				if err := s.publishLabels(); err != nil {
					s.log.Error().Err(err).Msg("failed to publish labels")
				}
			case <-watchCtx.Done():
				return
			}
		})
	}

	return nil
}

func (s *LabelService) Stop(ctx context.Context) error {
	s.watchCancelFunc()

	s.wg.Wait()

	return nil
}

func (s *LabelService) publishLabels() error {
	labels := make(map[string]string)

	for _, provider := range s.labelProviders {
		if provider == nil {
			continue
		}

		providerLabels := provider.GetNodeLabels()
		maps.Copy(labels, providerLabels)
	}

	return s.labelPublisher.PublishNodeLabels(labels)
}
