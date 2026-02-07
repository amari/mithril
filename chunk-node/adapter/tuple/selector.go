package tuple

// A Selectable can be converted to a FoundationDB KeySelector. All functions in
// the FoundationDB API that resolve a key selector to a key accept Selectable.
type Selectable interface {
	FDBKeySelector() KeySelector
}

// KeySelector represents a description of a key in a FoundationDB database. A
// KeySelector may be resolved to a specific key with the GetKey method, or used
// as the endpoints of a SelectorRange to be used with a GetRange function.
//
// The most common key selectors are constructed with the functions documented
// below. For details of how KeySelectors are specified and resolved, see
// https://apple.github.io/foundationdb/developer-guide.html#key-selectors.
type KeySelector struct {
	Key     KeyConvertible
	OrEqual bool
	Offset  int
}

func (ks KeySelector) FDBKeySelector() KeySelector {
	return ks
}

// LastLessThan returns the KeySelector specifying the lexicographically greatest
// key present in the database which is lexicographically strictly less than the
// given key.
func LastLessThan(key KeyConvertible) KeySelector {
	return KeySelector{key, false, 0}
}

// LastLessOrEqual returns the KeySelector specifying the lexicographically
// greatest key present in the database which is lexicographically less than or
// equal to the given key.
func LastLessOrEqual(key KeyConvertible) KeySelector {
	return KeySelector{key, true, 0}
}

// FirstGreaterThan returns the KeySelector specifying the lexicographically least
// key present in the database which is lexicographically strictly greater than
// the given key.
func FirstGreaterThan(key KeyConvertible) KeySelector {
	return KeySelector{key, true, 1}
}

// FirstGreaterOrEqual returns the KeySelector specifying the lexicographically
// least key present in the database which is lexicographically greater than or
// equal to the given key.
func FirstGreaterOrEqual(key KeyConvertible) KeySelector {
	return KeySelector{key, false, 1}
}
