package node

import (
	"context"
	"hash/fnv"
	"math/rand/v2"
	"os"
	"time"

	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/port"
)

type SeedGenerator struct{}

var _ port.NodeSeedGenerator = (*SeedGenerator)(nil)

func NewSeedGenerator() port.NodeSeedGenerator {
	return &SeedGenerator{}
}

// GenerateNodeSeed implements NodeSeedGenerator
func (sg *SeedGenerator) GenerateNodeSeed(ctx context.Context) (domain.NodeSeed, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	// We seed the PCG RNG using the current time in nanoseconds.
	// PCG requires two uint64 values: a seed and a stream.
	// Using time-based entropy ensures each invocation produces a different sequence,
	// while shifting the timestamp for the stream value guarantees the two inputs differ.
	nowNano := uint64(time.Now().UnixNano())
	r := rand.New(rand.NewPCG(nowNano, nowNano>>1))

	// To avoid multiple machines generating identical seeds at the same moment,
	// we fold in machine-unique entropy. Hashing the hostname gives us a stable,
	// uniform 64-bit value that we can mix with the RNG output.
	h := fnv.New64a()
	h.Write([]byte(hostname))
	hostHash := h.Sum64()

	// We generate a fixed 32-byte seed. Each byte is derived from the RNG output
	// and XORed with the hostname hash. XOR mixing ensures that:
	//   - the seed differs across machines,
	//   - the seed differs across calls,
	//   - and the RNG output is not directly exposed.
	buf := make([]byte, 32)
	for i := range buf {
		buf[i] = byte(r.Uint64() ^ hostHash)
	}

	return domain.NodeSeed(buf), nil
}
