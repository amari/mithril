package adaptersetcd

import "errors"

var (
	// JSON encoding/decoding failures.
	ErrNodeClaimEncodingFailed = errors.New("failed to encode node claim")
	ErrNodeClaimDecodingFailed = errors.New("failed to decode node claim")
	// Claim not found.
	ErrNodeClaimNotFound = errors.New("node claim not found")

	// Network or retry exhaustion.
	ErrRetryExhausted = errors.New("etcd retry limit reached")

	// Unexpected response shape from etcd.
	ErrUnexpectedResponse = errors.New("unexpected etcd response")

	// Protobuf encoding/decoding failures.
	ErrLabeledVolumeBitsetEncodingFailed = errors.New("failed to encode node claim")
	ErrLabeledVolumeBitsetDecodingFailed = errors.New("failed to decode node claim")
)
