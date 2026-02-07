package picker

// Linear implements a linear probing strategy.
// It calculates the next index to probe based on the total number of items,
// the starting index, and the current attempt number.
func LinearProbe(total, start, attempt int) int {
	if total <= 0 {
		return -1
	}

	return (start + attempt) % total
}
