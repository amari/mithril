package remotechunknode

import (
	"context"
	"time"
)

type retryPolicyKey struct{}

// RetryPolicy defines retry behavior for remote chunk operations.
//
// Retry logic applies to:
// - ERROR_CODE_WRONG_NODE (resolution races, node migrations)
// - Transient transport errors (Unavailable, DeadlineExceeded, ResourceExhausted)
// - Stream interruptions (premature EOF, connection drops)
//
// Non-retryable errors:
// - NOT_FOUND, VERSION_MISMATCH, PERMISSION_DENIED (business logic errors)
// - Context cancellation (caller explicitly canceled)
//
// The policy uses exponential backoff with jitter to avoid thundering herd.
type RetryPolicy struct {
	// InitialInterval is the backoff interval for the first retry.
	// If BackoffCoefficient is 1.0 then it is used for all retries.
	// Default: 100ms
	InitialInterval time.Duration

	// BackoffCoefficient is used to calculate the next retry backoff interval.
	// The next retry interval is previous interval multiplied by this coefficient.
	// Must be 1 or larger. Default: 2.0
	BackoffCoefficient float64

	// MaximumInterval is the cap for backoff interval between retries.
	// Default: 10s
	MaximumInterval time.Duration

	// MaximumAttempts is the maximum number of attempts.
	// When exceeded, retries stop even if context is not expired.
	// If 0, unlimited attempts (relies on context timeout). Default: 3
	MaximumAttempts int32
}

// WithRetryPolicy attaches a retry policy to the context.
// Use this to override the default retry behavior for remote chunk operations.
//
// Example:
//
//	// Aggressive retry for critical path
//	ctx = remotechunknode.WithRetryPolicy(ctx, remotechunknode.RetryPolicy{
//	    InitialInterval: 50 * time.Millisecond,
//	    MaximumAttempts: 5,
//	})
//
//	// Conservative retry for background jobs
//	ctx = remotechunknode.WithRetryPolicy(ctx, remotechunknode.RetryPolicy{
//	    InitialInterval: 1 * time.Second,
//	    MaximumAttempts: 2,
//	})
func WithRetryPolicy(ctx context.Context, policy RetryPolicy) context.Context {
	return context.WithValue(ctx, retryPolicyKey{}, policy)
}

// GetRetryPolicy extracts retry policy from context, or returns defaults.
// This function is used internally by RemoteChunkClient implementations.
//
// Default policy:
// - InitialInterval: 100ms
// - BackoffCoefficient: 2.0
// - MaximumInterval: 10s
// - MaximumAttempts: 3
func GetRetryPolicy(ctx context.Context) RetryPolicy {
	if policy, ok := ctx.Value(retryPolicyKey{}).(RetryPolicy); ok {
		// Apply defaults for zero values
		if policy.InitialInterval == 0 {
			policy.InitialInterval = 100 * time.Millisecond
		}
		if policy.BackoffCoefficient == 0 {
			policy.BackoffCoefficient = 2.0
		}
		if policy.MaximumInterval == 0 {
			policy.MaximumInterval = 10 * time.Second
		}
		if policy.MaximumAttempts == 0 {
			policy.MaximumAttempts = 3
		}
		return policy
	}

	// Default policy
	return RetryPolicy{
		InitialInterval:    100 * time.Millisecond,
		BackoffCoefficient: 2.0,
		MaximumInterval:    10 * time.Second,
		MaximumAttempts:    3,
	}
}
