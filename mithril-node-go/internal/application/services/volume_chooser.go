package applicationservices

import (
	"cmp"
	"math/rand/v2"
	"slices"
	"sync"

	"github.com/amari/mithril/mithril-node-go/internal/domain"
)

// PowerOfTwo implements the "Power of Two Choices" volume selection algorithm.
type PowerOfTwoChoicesVolumeChooser struct {
	Rand    *rand.Rand
	Compare func(i, j domain.VolumeID) int

	mu  sync.RWMutex
	ids []domain.VolumeID
}

var _ domain.VolumeChooser = (*PowerOfTwoChoicesVolumeChooser)(nil)

func (c *PowerOfTwoChoicesVolumeChooser) Choose(options domain.ChooseVolumeOptions) (domain.VolumeID, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	total := len(c.ids)
	if total == 0 {
		return 0, ErrVolumeSelectionFailed
	}

	probe := options.Probe
	if probe == nil {
		probe = domain.LinearProbe
	}

	filter := options.Filter

	// Ensure at least one attempt
	maxAttempts := max(min(total, options.MaxAttempts), 1)

	compare := c.Compare
	if compare == nil {
		compare = cmp.Compare
	}

	randIntN := rand.IntN
	if c.Rand != nil {
		randIntN = c.Rand.IntN
	}

	// Start indexes
	i := randIntN(total)
	j := randIntN(total)
	if i == j {
		j = randIntN(total)
	}

	// We need two candidates
	var first, second domain.VolumeID

	for attempt := range maxAttempts {
		idx := probe(total, i, attempt)
		if idx < 0 || idx >= total {
			continue
		}

		id := c.ids[idx]

		if filter != nil && !filter(id) {
			continue
		}

		first = id

		break
	}

	if first == 0 {
		return 0, ErrVolumeSelectionFailed
	}

	for attempt := range maxAttempts {
		idx := probe(total, j, attempt)
		if idx < 0 || idx >= total {
			continue
		}

		id := c.ids[idx]

		if filter != nil && !filter(id) {
			continue
		}

		second = id

		break
	}

	if second == 0 {
		// Only one candidate found
		return first, nil
	}

	// Compare the two candidates
	if compare(first, second) <= 0 {
		return first, nil
	} else {
		return second, nil
	}
}

func (c *PowerOfTwoChoicesVolumeChooser) Reset(ids []domain.VolumeID) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(ids) == 0 {
		c.ids = nil
		return
	}

	c.ids = slices.Clone(ids)

	slices.Sort(c.ids)
}

func (c *PowerOfTwoChoicesVolumeChooser) Refresh(id domain.VolumeID) {
	// no-op
}

// Random implements selects volume ids randomly.
type RandomVolumeChooser struct {
	Rand *rand.Rand

	mu  sync.RWMutex
	ids []domain.VolumeID
}

var _ domain.VolumeChooser = (*RandomVolumeChooser)(nil)

func NewRandomVolumeChooser(r *rand.Rand) *RandomVolumeChooser {
	return &RandomVolumeChooser{
		Rand: r,
	}
}

func (c *RandomVolumeChooser) Choose(options domain.ChooseVolumeOptions) (domain.VolumeID, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	total := len(c.ids)
	if total == 0 {
		return 0, ErrVolumeSelectionFailed
	}

	probe := options.Probe
	if probe == nil {
		probe = domain.LinearProbe
	}

	filter := options.Filter

	// Ensure at least one attempt
	maxAttempts := max(min(total, options.MaxAttempts), 1)

	randIntN := rand.IntN
	if c.Rand != nil {
		randIntN = c.Rand.IntN
	}

	start := randIntN(total)

	for attempt := range maxAttempts {
		idx := probe(total, start, attempt)
		if idx < 0 || idx >= total {
			continue
		}

		id := c.ids[idx]

		if filter != nil && !filter(id) {
			continue
		}

		return id, nil
	}

	return 0, ErrVolumeSelectionFailed
}

func (c *RandomVolumeChooser) Reset(ids []domain.VolumeID) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(ids) == 0 {
		c.ids = nil
		return
	}

	c.ids = slices.Clone(ids)

	slices.Sort(c.ids)
}

func (c *RandomVolumeChooser) Refresh(id domain.VolumeID) {
	// no-op
}

// RoundRobinVolumeChooser selects volume ids in a round-robin fashion.
type RoundRobinVolumeChooser struct {
	mu      sync.RWMutex
	ids     []domain.VolumeID
	nextIdx int
}

var _ domain.VolumeChooser = (*RoundRobinVolumeChooser)(nil)

func NewRoundRobinVolumeChooser() *RoundRobinVolumeChooser {
	return &RoundRobinVolumeChooser{}
}

func (c *RoundRobinVolumeChooser) Choose(options domain.ChooseVolumeOptions) (domain.VolumeID, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	total := len(c.ids)
	if total == 0 {
		return 0, ErrVolumeSelectionFailed
	}

	probe := options.Probe
	if probe == nil {
		probe = domain.LinearProbe
	}

	filter := options.Filter

	// Ensure at least one attempt
	maxAttempts := max(min(total, options.MaxAttempts), 1)

	for attempt := range maxAttempts {
		idx := probe(total, c.nextIdx, attempt)
		if idx < 0 || idx >= total {
			continue
		}

		id := c.ids[idx]

		if filter != nil && !filter(id) {
			continue
		}

		c.nextIdx = (idx + 1) % total

		return id, nil
	}

	return 0, ErrVolumeSelectionFailed
}

func (c *RoundRobinVolumeChooser) Reset(ids []domain.VolumeID) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(ids) == 0 {
		c.ids = nil
		return
	}

	c.ids = slices.Clone(ids)

	slices.Sort(c.ids)
}

func (c *RoundRobinVolumeChooser) Refresh(id domain.VolumeID) {
	// no-op
}
