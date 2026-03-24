package adaptersfilestore

import (
	"sync"

	"github.com/amari/mithril/mithril-node-go/internal/domain"
)

type ChunkStorageHealthController struct {
	mu       sync.RWMutex
	health   domain.VolumeHealth
	failedCh chan struct{}
}

var _ domain.VolumeHealthProvider = (*ChunkStorageHealthController)(nil)

func NewChunkStorageHealthController() *ChunkStorageHealthController {
	return &ChunkStorageHealthController{
		health:   domain.VolumeUnknown,
		failedCh: make(chan struct{}),
	}
}

func (p *ChunkStorageHealthController) Get() domain.VolumeHealth {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.health
}

func (p *ChunkStorageHealthController) Failed() <-chan struct{} {
	return p.failedCh
}

func (p *ChunkStorageHealthController) IsFailed() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.health == domain.VolumeFailed
}
