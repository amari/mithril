package adaptersfilestore

import (
	"context"

	"github.com/amari/mithril/mithril-node-go/internal/domain"
)

type NopVolumeStatisticsProvider[T any] struct{}

func NewNopVolumeStatisticsProvider[T any]() (*NopVolumeStatisticsProvider[T], error) {
	return &NopVolumeStatisticsProvider[T]{}, nil
}

func (p *NopVolumeStatisticsProvider[T]) start() error {
	return nil
}

func (p *NopVolumeStatisticsProvider[T]) stop() error {
	return nil
}

func (p *NopVolumeStatisticsProvider[T]) Get() domain.Sample[T] {
	return domain.Sample[T]{}
}

func (p *NopVolumeStatisticsProvider[T]) Watch(watchCtx context.Context) <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}
