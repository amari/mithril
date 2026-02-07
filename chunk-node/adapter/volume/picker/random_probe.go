package picker

import (
	"math/rand/v2"
	"time"
)

// Random selects a random index from the available total number of targets.
// It ignores the start and attempt parameters.
// If total is less than or equal to zero, it returns -1.
func RandomProbe(total, _start, _attempt int) int {
	if total <= 0 {
		return -1
	}

	return rand.IntN(total)
}

// ProbeWithRand returns a probe function that selects a random index
// from the available total number of targets.
// If rng is nil, a new random number generator seeded with the current time is used.
func ProbeWithRand(rng *rand.Rand) func(total, start, attempt int) int {
	if rng == nil {
		now := uint64(time.Now().UnixNano())
		rng = rand.New(rand.NewPCG(now, now<<1|1))
	}

	return func(total, _start, _attempt int) int {
		if total <= 0 {
			return -1
		}

		return rng.IntN(total)
	}
}
