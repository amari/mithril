package adapternode

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"time"

	"github.com/amari/mithril/chunk-node/domain"
	"github.com/amari/mithril/chunk-node/nodeerrors"
	"github.com/amari/mithril/chunk-node/port"
	"github.com/cenkalti/backoff/v5"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type IdentityAllocator struct {
	client *clientv3.Client
}

var _ port.NodeIdentityAllocator = (*IdentityAllocator)(nil)

func NewIdentityAllocator(client *clientv3.Client) port.NodeIdentityAllocator {
	return &IdentityAllocator{
		client: client,
	}
}

func nodeIdentityKey(nodeID uint32) string {
	return fmt.Sprintf("/claims/%08x", uint32(nodeID))
}

func (a *IdentityAllocator) AllocateNodeIdentity(ctx context.Context, seed domain.NodeSeed) (*domain.NodeIdentity, error) {
	// Deterministically derive RNG seeds from the full node seed.
	sum := sha256.Sum256(seed[:])
	seed1 := binary.BigEndian.Uint64(sum[0:8])
	seed2 := binary.BigEndian.Uint64(sum[8:16])

	rng := rand.New(rand.NewPCG(seed1, seed2))

	// Deterministic per-seed proof (stable across restarts with the same seed).
	var proof [32]byte
	binary.LittleEndian.PutUint64(proof[:8], rng.Uint64())
	binary.LittleEndian.PutUint64(proof[8:16], rng.Uint64())
	binary.LittleEndian.PutUint64(proof[16:24], rng.Uint64())
	binary.LittleEndian.PutUint64(proof[24:32], rng.Uint64())

	proofStr := base64.StdEncoding.EncodeToString(proof[:])
	proofStrBytes := []byte(proofStr)

	identity := &domain.NodeIdentity{
		Proof: proof[:],
	}

	// Candidate-ID salt buffer (reused, no allocations).
	var saltBuf [16]byte

	// Backoff used only for retryable (network-ish) errors talking to etcd.
	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = 50 * time.Millisecond
	bo.MaxInterval = 2 * time.Second

	// Hasher used for generating node id
	hasher := sha256.New()

	for ctx.Err() == nil {
		bo.Reset()
		hasher.Reset()

		// Next candidate ID from deterministic RNG stream.
		binary.LittleEndian.PutUint64(saltBuf[:8], rng.Uint64())
		binary.LittleEndian.PutUint64(saltBuf[8:], rng.Uint64())

		hasher.Write(seed)
		hasher.Write(saltBuf[:])

		idSum := hasher.Sum(nil)
		nodeID := binary.LittleEndian.Uint32(idSum[0:4])

		key := nodeIdentityKey(nodeID)
		identity.NodeID = domain.NodeID(nodeID)

		// Claim-or-confirm:
		// - If key doesn't exist, atomically claim it with our proofStr.
		// - Else, read existing value. If it matches our proofStr, we already own it.
		// - Else, collision: try next nodeID.
		for {
			if err := ctx.Err(); err != nil {
				return nil, err
			}

			resp, err := a.client.Txn(ctx).
				If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
				Then(clientv3.OpPut(key, proofStr)).
				Else(clientv3.OpGet(key)).
				Commit()

			if err != nil {
				// Retry a bounded amount for network-type errors; return others immediately.
				var netErr net.Error
				if errors.As(err, &netErr) {
					d := bo.NextBackOff()
					if d == backoff.Stop {
						return nil, fmt.Errorf("%w: %w", nodeerrors.ErrIdentityAllocationFailed, err)
					}

					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case <-time.After(d):
					}
					continue
				}

				return nil, err
			}

			// Successful claim on missing key.
			if resp.Succeeded {
				bo.Reset()
				return identity, nil
			}

			// Existing key path: we expect the Else OpGet response.
			if len(resp.Responses) != 1 {
				// Unexpected response shape; treat as collision and move on.
				break
			}

			getResp := resp.Responses[0].GetResponseRange()
			if getResp == nil || len(getResp.Kvs) == 0 {
				// Rare race (deleted between compare/get). Retry same candidate immediately.
				continue
			}

			if bytes.Equal(getResp.Kvs[0].Value, proofStrBytes) {
				// We already own it (idempotent success).
				bo.Reset()
				return identity, nil
			}

			// Collision: someone else owns this nodeID.
			break
		}
	}

	return nil, ctx.Err()
}

func (a *IdentityAllocator) ValidateNodeIdentity(ctx context.Context, identity *domain.NodeIdentity) error {
	if identity == nil {
		return nodeerrors.ErrIdentityInvalid
	}
	if len(identity.Proof) != 32 {
		return nodeerrors.ErrIdentityInvalid
	}

	proofStr := base64.StdEncoding.EncodeToString(identity.Proof)
	proofStrBytes := []byte(proofStr)

	resp, err := a.client.KV.Get(ctx, nodeIdentityKey(uint32(identity.NodeID)))
	if err != nil {
		return err
	}

	if len(resp.Kvs) == 0 {
		return nodeerrors.ErrIdentityNotFound
	}

	if !bytes.Equal(resp.Kvs[0].Value, proofStrBytes) {
		return nodeerrors.ErrIdentityProofMismatch
	}

	return nil
}
