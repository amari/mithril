package picker

import (
	"slices"
	"sync"

	"github.com/amari/mithril/chunk-node/domain"
	chunkstoreerrors "github.com/amari/mithril/chunk-node/errors"
	"github.com/amari/mithril/chunk-node/port/volume"
)

// RoundRobin implements a volume picker that selects volumes in a round-robin fashion.
type RoundRobin struct {
	mu            sync.RWMutex
	nextIndex     int
	volumeIDSlice []domain.VolumeID
}

var _ volume.VolumePicker = (*RoundRobin)(nil)

func (rr *RoundRobin) PickVolumeID(opts volume.PickVolumeIDOptions) (domain.VolumeID, error) {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	total := len(rr.volumeIDSlice)
	if total == 0 {
		return 0, chunkstoreerrors.ErrVolumeNotFound
	}

	// Default probe function
	probeFunc := LinearProbe

	// Override probe function if provided
	if opts.ProbeFunc != nil {
		probeFunc = opts.ProbeFunc
	}

	filter := opts.Filter

	// Ensure at least one attempt
	maxAttempts := max(min(total, opts.MaxProbeAttempts), 1)

	for attempt := range maxAttempts {
		idx := probeFunc(total, rr.nextIndex, attempt)
		if idx < 0 || idx >= total {
			continue
		}

		vol := rr.volumeIDSlice[idx]

		if filter != nil && !filter.FilterVolumeIDPick(vol) {
			continue
		}

		rr.nextIndex = (idx + 1) % total

		return vol, nil
	}

	return 0, chunkstoreerrors.ErrVolumeNotFound
}

func (rr *RoundRobin) SetVolumeIDs(vols []domain.VolumeID) {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	rr.volumeIDSlice = slices.Clone(vols)

	slices.Sort(rr.volumeIDSlice)
}

func (rr *RoundRobin) UpdateVolumeID(_ domain.VolumeID) {}
