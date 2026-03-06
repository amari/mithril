package applicationservices

import "context"

type ClockFence interface {
	// Returns the persisted clock value expressed as milliseconds since the UNIX epoch.
	UnixMilli(ctx context.Context) (int64, error)
}
