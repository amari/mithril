package picker

// Quadratic calculates the next index to probe using quadratic probing.
// It takes the total number of slots, the starting index, and the current attempt number.
// It returns the next index to probe, or -1 if total is less than or equal to 0.
func QuadraticProbe(total, start, attempt int) int {
	if total <= 0 {
		return -1
	}

	step := attempt * attempt

	return (start + step) % total
}
