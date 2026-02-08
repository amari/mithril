package nodeerrors

import "errors"

var (
	// ErrIdentityNotFound indicates the node hasn't been assigned an identity yet.
	// The bootstrap process should allocate one.
	ErrIdentityNotFound = errors.New("node identity not found")
	// ErrIdentityInvalid indicates the stored identity is malformed.
	ErrIdentityInvalid = errors.New("node identity invalid")
	// ErrIdentityAllocationFailed indicates identity allocation failed after retries.
	// This typically means etcd is unavailable or the ID space is exhausted.
	ErrIdentityAllocationFailed = errors.New("node identity allocation failed")
	// ErrIdentityProofMismatch indicates the identity proof doesn't match.
	// This suggests data corruption or tampering.
	ErrIdentityProofMismatch = errors.New("node identity proof mismatch")
	// ErrSeedNotFound indicates no seed file exists.
	// A new seed should be generated during bootstrap.
	ErrSeedNotFound = errors.New("node seed not found")
)
