package picker

import (
	"cmp"
	"math/rand/v2"
	"slices"
	"sync"

	"github.com/amari/mithril/chunk-node/domain"
	chunkstoreerrors "github.com/amari/mithril/chunk-node/errors"
	"github.com/amari/mithril/chunk-node/port/volume"
)

// PowerOfTwo implements the "Power of Two Choices" volume selection algorithm.
type PowerOfTwo struct {
	Rand        *rand.Rand
	CompareFunc func(a, b domain.VolumeID) int

	mu            sync.RWMutex
	volumeIDSlice []domain.VolumeID
}

var _ volume.VolumePicker = (*PowerOfTwo)(nil)

func (p *PowerOfTwo) PickVolumeID(opts volume.PickVolumeIDOptions) (domain.VolumeID, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	total := len(p.volumeIDSlice)
	if total == 0 {
		return 0, chunkstoreerrors.ErrVolumeNotFound
	}

	// Default probe function
	probeFunc := LinearProbe
	if opts.ProbeFunc != nil {
		probeFunc = opts.ProbeFunc
	}

	// Default compare function: compare by Volume.ID
	compareFunc := p.CompareFunc
	if compareFunc == nil {
		compareFunc = func(a, b domain.VolumeID) int {
			return cmp.Compare(a, b)
		}
	}

	filter := opts.Filter

	// Ensure at least one attempt
	maxAttempts := max(min(total, opts.MaxProbeAttempts), 1)

	// RNG selection
	randIntNFunc := rand.IntN
	if p.Rand != nil {
		randIntNFunc = p.Rand.IntN
	}

	// Start indexes
	i := randIntNFunc(total)
	j := randIntNFunc(total)
	if i == j {
		j = randIntNFunc(total)
	}

	// We need two candidates
	var first, second domain.VolumeID

	for attempt := range maxAttempts {
		idx := probeFunc(total, i, attempt)
		if idx < 0 || idx >= total {
			continue
		}

		vol := p.volumeIDSlice[idx]

		if filter != nil && !filter.FilterVolumeIDPick(vol) {
			continue
		}

		first = vol
		break
	}

	if first == 0 {
		return 0, chunkstoreerrors.ErrVolumeNotFound
	}

	for attempt := range maxAttempts {
		idx := probeFunc(total, j, attempt)
		if idx < 0 || idx >= total {
			continue
		}

		vol := p.volumeIDSlice[idx]

		if filter != nil && !filter.FilterVolumeIDPick(vol) {
			continue
		}

		second = vol
		break
	}

	if second == 0 {
		// Only one candidate found
		return first, nil
	}

	// Compare the two candidates
	if compareFunc(first, second) <= 0 {
		return first, nil
	}

	return second, nil
}

func (p *PowerOfTwo) SetVolumeIDs(volumeIDs []domain.VolumeID) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.volumeIDSlice = slices.Clone(volumeIDs)

	// Sort volumes deterministically by ID
	slices.Sort(p.volumeIDSlice)
}

func (p *PowerOfTwo) UpdateVolumeID(_ domain.VolumeID) {
	// No-op for now, same as Random
}
