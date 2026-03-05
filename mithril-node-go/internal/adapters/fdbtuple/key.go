package fdbtuple

import (
	"bytes"
	"fmt"
)

// A KeyConvertible can be converted to a FoundationDB Key. All functions in the
// FoundationDB API that address a specific key accept a KeyConvertible.
type KeyConvertible interface {
	FDBKey() Key
}

// Key represents a FoundationDB key, a lexicographically-ordered sequence of
// bytes. Key implements the KeyConvertible interface.
type Key []byte

// FDBKey allows Key to (trivially) satisfy the KeyConvertible interface.
func (k Key) FDBKey() Key {
	return k
}

// String describes the key as a human readable string.
func (k Key) String() string {
	return Printable(k)
}

// Printable returns a human readable version of a byte array. The bytes that correspond with
// ASCII printable characters [32-127) are passed through. Other bytes are
// replaced with \x followed by a two character zero-padded hex code for byte.
func Printable(d []byte) string {
	buf := new(bytes.Buffer)
	for _, b := range d {
		if b >= 32 && b < 127 && b != '\\' {
			buf.WriteByte(b)
			continue
		}
		if b == '\\' {
			buf.WriteString("\\\\")
			continue
		}
		buf.WriteString(fmt.Sprintf("\\x%02x", b))
	}
	return buf.String()
}
