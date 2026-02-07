package errors

import "errors"

// Node Identity Errors
var (
	// ErrNodeIdentityNotFound indicates the node hasn't been assigned an identity yet.
	// The bootstrap process should allocate one.
	ErrNodeIdentityNotFound = errors.New("node identity not found")

	// ErrNodeIdentityInvalid indicates the stored identity is malformed.
	ErrNodeIdentityInvalid = errors.New("node identity invalid")

	// ErrNodeIdentityAllocationFailed indicates identity allocation failed after retries.
	// This typically means etcd is unavailable or the ID space is exhausted.
	ErrNodeIdentityAllocationFailed = errors.New("node identity allocation failed")

	// ErrNodeIdentityProofMismatch indicates the identity proof doesn't match.
	// This suggests data corruption or tampering.
	ErrNodeIdentityProofMismatch = errors.New("node identity proof mismatch")
)

// Node Seed Errors
var (
	// ErrNodeSeedNotFound indicates no seed file exists.
	// A new seed should be generated during bootstrap.
	ErrNodeSeedNotFound = errors.New("node seed not found")
)

type Node struct {
	ID []byte
}
