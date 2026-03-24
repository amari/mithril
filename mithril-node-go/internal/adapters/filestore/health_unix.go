//go:build unix
// +build unix

package adaptersfilestore

import (
	"errors"
	"fmt"

	"github.com/amari/mithril/mithril-node-go/internal/domain"
	"golang.org/x/sys/unix"
)

func (p *ChunkStorageHealthController) handleError(err error) error {
	p.mu.RLock()
	health := p.health
	p.mu.RUnlock()

	// degraded is a transient state. failed is a terminal state.
	switch health {
	case domain.VolumeFailed:
		return fmt.Errorf("%w: %w", domain.ErrVolumeFailed, err)
	default:
		if errors.Is(err, unix.EIO) || errors.Is(err, ErrFSFsyncFailed) {
			p.mu.Lock()
			if p.health != domain.VolumeFailed {
				p.health = domain.VolumeFailed

				close(p.failedCh)
			}
			p.mu.Unlock()

			return fmt.Errorf("%w: %w", domain.ErrVolumeFailed, err)
		} else if errors.Is(err, unix.EDQUOT) || errors.Is(err, unix.ENOSPC) {
			return fmt.Errorf("%w: %w", domain.ErrVolumeDegraded, err)
		}
	}

	return err
}
