package portvolume

import "github.com/amari/mithril/chunk-node/domain"

type Sample[T any] = domain.Sample[T]

type Sampler[T any] interface {
	GetSample() Sample[T]
	Close() error
}
