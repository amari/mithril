package picker

import (
	"math/rand/v2"
	"slices"
	"sync"

	"github.com/amari/mithril/chunk-node/domain"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
	"github.com/amari/mithril/chunk-node/volumeerrors"
)

// Random implements a volume scheduler that selects volumes randomly.
type Random struct {
	mu            sync.RWMutex
	volumeIDSlice []domain.VolumeID

	Rand *rand.Rand
}

var _ portvolume.VolumePicker = (*Random)(nil)

func (r *Random) PickVolumeID(opts portvolume.PickVolumeIDOptions) (domain.VolumeID, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	total := len(r.volumeIDSlice)
	if total == 0 {
		return 0, volumeerrors.ErrNotFound
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

	// Start from a random index
	randIntNFunc := rand.IntN
	if r.Rand != nil {
		randIntNFunc = r.Rand.IntN
	}
	start := randIntNFunc(total)

	for attempt := range maxAttempts {
		idx := probeFunc(total, start, attempt)
		if idx < 0 || idx >= total {
			continue
		}

		vol := r.volumeIDSlice[idx]

		if filter != nil && !filter.FilterVolumeIDPick(vol) {
			continue
		}

		return vol, nil
	}

	return 0, volumeerrors.ErrNotFound
}

func (r *Random) SetVolumeIDs(volumeIDs []domain.VolumeID) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.volumeIDSlice = slices.Clone(volumeIDs)

	slices.Sort(r.volumeIDSlice)
}

func (r *Random) UpdateVolumeID(_ domain.VolumeID) {}
