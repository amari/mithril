package domain

import "time"

// A sample of a particular statistic type T.
type Sample[T any] struct {
	// The sampled value
	Value T
	// A monotonic increasing epoch counter incremented whenever the underlying provider attempts to update stats.
	Epoch uint64
	// The time at which the sample was taken.
	Time time.Time
	// Any error encountered while obtaining the sample.
	Error error
}

func (s *Sample[T]) IsZero() bool {
	return s == nil || (s.Time.IsZero() || s.Epoch == 0)
}

func (s *Sample[T]) Valid() bool {
	return s != nil && s.Error == nil && !s.Time.IsZero() && s.Epoch != 0
}

func (s *Sample[T]) Compare(t *Sample[T]) int {
	switch {
	case s == nil && t == nil:
		return 0
	case s == nil:
		return -1
	case t == nil:
		return 1
	}

	if !s.Time.Equal(t.Time) {
		if s.Time.Before(t.Time) {
			return -1
		}
		return 1
	}

	switch {
	case s.Epoch < t.Epoch:
		return -1
	case s.Epoch > t.Epoch:
		return 1
	default:
		return 0
	}
}

func (s *Sample[T]) After(t *Sample[T]) bool {
	return s.Compare(t) > 0
}

func (s *Sample[T]) Before(t *Sample[T]) bool {
	return s.Compare(t) < 0
}
