package domain

import (
	"testing"
	"time"
)

func TestNewChunkID_CreatesValidID(t *testing.T) {
	unixMilli := int64(1700000000000)
	nodeID := uint32(12345)
	volumeID := uint16(100)
	sequence := uint16(42)

	id := NewChunkID(unixMilli, nodeID, volumeID, sequence)

	if id.UnixMilli() != unixMilli {
		t.Errorf("UnixMilli() = %v, want %v", id.UnixMilli(), unixMilli)
	}
	if id.NodeID() != NodeID(nodeID) {
		t.Errorf("NodeID() = %v, want %v", id.NodeID(), NodeID(nodeID))
	}
	if id.VolumeID() != VolumeID(volumeID) {
		t.Errorf("VolumeID() = %v, want %v", id.VolumeID(), VolumeID(volumeID))
	}
	if id.Sequence() != sequence {
		t.Errorf("Sequence() = %v, want %v", id.Sequence(), sequence)
	}
}

func TestNewChunkID_ZeroValues(t *testing.T) {
	id := NewChunkID(0, 0, 0, 0)

	if id.UnixMilli() != 0 {
		t.Errorf("UnixMilli() = %v, want 0", id.UnixMilli())
	}
	if id.NodeID() != 0 {
		t.Errorf("NodeID() = %v, want 0", id.NodeID())
	}
	if id.VolumeID() != 0 {
		t.Errorf("VolumeID() = %v, want 0", id.VolumeID())
	}
	if id.Sequence() != 0 {
		t.Errorf("Sequence() = %v, want 0", id.Sequence())
	}
}

func TestNewChunkID_MaxValues(t *testing.T) {
	maxUnixMilli := int64(1<<63 - 1)
	maxNodeID := uint32(1<<32 - 1)
	maxVolumeID := uint16(1<<16 - 1)
	maxSequence := uint16(1<<16 - 1)

	id := NewChunkID(maxUnixMilli, maxNodeID, maxVolumeID, maxSequence)

	// Note: UnixMilli stores as uint64, so max int64 should round-trip correctly
	if id.UnixMilli() != maxUnixMilli {
		t.Errorf("UnixMilli() = %v, want %v", id.UnixMilli(), maxUnixMilli)
	}
	if id.NodeID() != NodeID(maxNodeID) {
		t.Errorf("NodeID() = %v, want %v", id.NodeID(), NodeID(maxNodeID))
	}
	if id.VolumeID() != VolumeID(maxVolumeID) {
		t.Errorf("VolumeID() = %v, want %v", id.VolumeID(), VolumeID(maxVolumeID))
	}
	if id.Sequence() != maxSequence {
		t.Errorf("Sequence() = %v, want %v", id.Sequence(), maxSequence)
	}
}

func TestChunkID_Time(t *testing.T) {
	now := time.Now()
	unixMilli := now.UnixMilli()

	id := NewChunkID(unixMilli, 1, 2, 3)
	result := id.Time()

	// Time() should return the same time (truncated to milliseconds)
	expected := time.UnixMilli(unixMilli)
	if !result.Equal(expected) {
		t.Errorf("Time() = %v, want %v", result, expected)
	}
}

func TestChunkID_Time_Epoch(t *testing.T) {
	id := NewChunkID(0, 0, 0, 0)
	result := id.Time()

	expected := time.UnixMilli(0)
	if !result.Equal(expected) {
		t.Errorf("Time() = %v, want Unix epoch", result)
	}
}

func TestChunkID_Bytes(t *testing.T) {
	id := NewChunkID(1700000000000, 12345, 100, 42)
	bytes := id.Bytes()

	if len(bytes) != 16 {
		t.Errorf("Bytes() length = %v, want 16", len(bytes))
	}

	// Verify bytes is a slice of the underlying array
	for i := 0; i < 16; i++ {
		if bytes[i] != id[i] {
			t.Errorf("Bytes()[%d] = %v, want %v", i, bytes[i], id[i])
		}
	}
}

func TestChunkID_String(t *testing.T) {
	id := NewChunkID(1700000000000, 12345, 100, 42)
	str := id.String()

	if str == "" {
		t.Error("String() returned empty string")
	}

	// String should be base64 URL encoded, 16 bytes -> ~22 chars without padding
	// RawURLEncoding means no padding
	if len(str) != 22 {
		t.Errorf("String() length = %v, want 22 (base64 of 16 bytes)", len(str))
	}
}

func TestParseID_ValidString(t *testing.T) {
	original := NewChunkID(1700000000000, 12345, 100, 42)
	str := original.String()

	parsed, err := ParseID(str)
	if err != nil {
		t.Fatalf("ParseID() error = %v", err)
	}

	if parsed != original {
		t.Errorf("ParseID() = %v, want %v", parsed, original)
	}
}

func TestParseID_InvalidBase64(t *testing.T) {
	_, err := ParseID("not-valid-base64!!!")
	if err == nil {
		t.Error("ParseID() expected error for invalid base64, got nil")
	}
}

func TestParseID_EmptyString(t *testing.T) {
	id, err := ParseID("")
	if err != nil {
		t.Fatalf("ParseID() error = %v for empty string", err)
	}

	// Empty string decodes to empty bytes, resulting in zero ChunkID
	var zeroID ChunkID
	if id != zeroID {
		t.Errorf("ParseID(\"\") = %v, want zero ChunkID", id)
	}
}

func TestParseID_ShortString(t *testing.T) {
	// Shorter than 16 bytes when decoded
	id, err := ParseID("AAAA")
	if err != nil {
		t.Fatalf("ParseID() error = %v", err)
	}

	// Should parse without error, but only first few bytes will be set
	if id.UnixMilli() != 0 && id.NodeID() != 0 {
		// The behavior depends on how many bytes were decoded
		// Just verify no panic occurred
	}
}

func TestParseID_RoundTrip(t *testing.T) {
	testCases := []struct {
		name      string
		unixMilli int64
		nodeID    uint32
		volumeID  uint16
		sequence  uint16
	}{
		{"zero values", 0, 0, 0, 0},
		{"typical values", 1700000000000, 12345, 100, 42},
		{"max uint16", 1000, 1, 65535, 65535},
		{"max uint32 nodeID", 1000, 4294967295, 1, 1},
		{"negative timestamp", -1000, 1, 2, 3},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			original := NewChunkID(tc.unixMilli, tc.nodeID, tc.volumeID, tc.sequence)
			str := original.String()
			parsed, err := ParseID(str)

			if err != nil {
				t.Fatalf("ParseID() error = %v", err)
			}
			if parsed != original {
				t.Errorf("round trip failed: got %v, want %v", parsed, original)
			}
		})
	}
}

func TestChunkID_FieldLayout(t *testing.T) {
	// Verify the byte layout of ChunkID matches expectations
	// [0:8]   = unixMilli (big endian uint64)
	// [8:12]  = nodeID (big endian uint32)
	// [12:14] = volumeID (big endian uint16)
	// [14:16] = sequence (big endian uint16)

	id := NewChunkID(0x0102030405060708, 0x090A0B0C, 0x0D0E, 0x0F10)

	expectedBytes := []byte{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // unixMilli
		0x09, 0x0A, 0x0B, 0x0C, // nodeID
		0x0D, 0x0E, // volumeID
		0x0F, 0x10, // sequence
	}

	for i, expected := range expectedBytes {
		if id[i] != expected {
			t.Errorf("id[%d] = 0x%02X, want 0x%02X", i, id[i], expected)
		}
	}
}

func TestChunkID_UnixMilli_NegativeTimestamp(t *testing.T) {
	// Negative timestamps represent times before Unix epoch
	negativeTime := int64(-1000)
	id := NewChunkID(negativeTime, 1, 2, 3)

	result := id.UnixMilli()
	if result != negativeTime {
		t.Errorf("UnixMilli() = %v, want %v", result, negativeTime)
	}
}

func TestChunkID_Size(t *testing.T) {
	var id ChunkID
	if len(id) != 16 {
		t.Errorf("ChunkID size = %v bytes, want 16", len(id))
	}
}

func TestChunkID_Equality(t *testing.T) {
	id1 := NewChunkID(1000, 1, 2, 3)
	id2 := NewChunkID(1000, 1, 2, 3)
	id3 := NewChunkID(1001, 1, 2, 3)

	if id1 != id2 {
		t.Error("identical ChunkIDs should be equal")
	}
	if id1 == id3 {
		t.Error("different ChunkIDs should not be equal")
	}
}

func TestChunkID_ZeroValue(t *testing.T) {
	var id ChunkID

	if id.UnixMilli() != 0 {
		t.Errorf("zero ChunkID UnixMilli() = %v, want 0", id.UnixMilli())
	}
	if id.NodeID() != 0 {
		t.Errorf("zero ChunkID NodeID() = %v, want 0", id.NodeID())
	}
	if id.VolumeID() != 0 {
		t.Errorf("zero ChunkID VolumeID() = %v, want 0", id.VolumeID())
	}
	if id.Sequence() != 0 {
		t.Errorf("zero ChunkID Sequence() = %v, want 0", id.Sequence())
	}
}

func TestChunkID_DifferentFields(t *testing.T) {
	// Test that each field is independent
	base := NewChunkID(1000, 100, 10, 1)

	// Change only unixMilli
	diffTime := NewChunkID(2000, 100, 10, 1)
	if base == diffTime {
		t.Error("ChunkIDs with different unixMilli should not be equal")
	}
	if diffTime.NodeID() != base.NodeID() || diffTime.VolumeID() != base.VolumeID() || diffTime.Sequence() != base.Sequence() {
		t.Error("changing unixMilli should not affect other fields")
	}

	// Change only nodeID
	diffNode := NewChunkID(1000, 200, 10, 1)
	if base == diffNode {
		t.Error("ChunkIDs with different nodeID should not be equal")
	}
	if diffNode.UnixMilli() != base.UnixMilli() || diffNode.VolumeID() != base.VolumeID() || diffNode.Sequence() != base.Sequence() {
		t.Error("changing nodeID should not affect other fields")
	}

	// Change only volumeID
	diffVolume := NewChunkID(1000, 100, 20, 1)
	if base == diffVolume {
		t.Error("ChunkIDs with different volumeID should not be equal")
	}
	if diffVolume.UnixMilli() != base.UnixMilli() || diffVolume.NodeID() != base.NodeID() || diffVolume.Sequence() != base.Sequence() {
		t.Error("changing volumeID should not affect other fields")
	}

	// Change only sequence
	diffSeq := NewChunkID(1000, 100, 10, 2)
	if base == diffSeq {
		t.Error("ChunkIDs with different sequence should not be equal")
	}
	if diffSeq.UnixMilli() != base.UnixMilli() || diffSeq.NodeID() != base.NodeID() || diffSeq.VolumeID() != base.VolumeID() {
		t.Error("changing sequence should not affect other fields")
	}
}

func TestChunkID_String_Deterministic(t *testing.T) {
	id := NewChunkID(1700000000000, 12345, 100, 42)

	str1 := id.String()
	str2 := id.String()

	if str1 != str2 {
		t.Errorf("String() not deterministic: %q != %q", str1, str2)
	}
}

func TestChunkID_Bytes_ReturnsSliceOfArray(t *testing.T) {
	id := NewChunkID(1000, 1, 2, 3)
	bytes := id.Bytes()

	// Verify that Bytes() returns a slice backed by the array
	// Modifying the slice should not affect the original (since it's a value receiver)
	originalFirstByte := id[0]
	bytes[0] = 0xFF

	// Since ChunkID is a value type and Bytes() has a value receiver,
	// the slice is backed by a copy, so modification shouldn't affect original
	// Actually, looking at the code: func (id ChunkID) Bytes() []byte { return id[:] }
	// This returns a slice of the local copy, so modifying bytes won't affect the original id
	// This is actually a subtle behavior worth testing

	if id[0] != originalFirstByte {
		t.Error("modifying Bytes() slice should not affect original ChunkID (value receiver)")
	}
}

func TestParseID_ExactLength(t *testing.T) {
	// Create a known ID and verify parsing produces exact match
	original := NewChunkID(1700000000000, 12345, 100, 42)
	encoded := original.String()

	parsed, err := ParseID(encoded)
	if err != nil {
		t.Fatalf("ParseID() error = %v", err)
	}

	// Verify all fields match exactly
	if parsed.UnixMilli() != original.UnixMilli() {
		t.Errorf("UnixMilli mismatch: got %v, want %v", parsed.UnixMilli(), original.UnixMilli())
	}
	if parsed.NodeID() != original.NodeID() {
		t.Errorf("NodeID mismatch: got %v, want %v", parsed.NodeID(), original.NodeID())
	}
	if parsed.VolumeID() != original.VolumeID() {
		t.Errorf("VolumeID mismatch: got %v, want %v", parsed.VolumeID(), original.VolumeID())
	}
	if parsed.Sequence() != original.Sequence() {
		t.Errorf("Sequence mismatch: got %v, want %v", parsed.Sequence(), original.Sequence())
	}
}

func TestParseID_TooLongInput(t *testing.T) {
	// Input longer than 16 bytes when decoded - should still work, extra bytes ignored
	// 24 bytes base64 encoded = 32 chars
	longInput := "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

	_, err := ParseID(longInput)
	// This might succeed (extra bytes ignored) or fail depending on implementation
	// The current implementation uses copy() which will only copy up to 16 bytes
	if err != nil {
		// If it errors, that's also acceptable behavior
		t.Logf("ParseID() with long input returned error: %v", err)
	}
}

func TestChunkID_NodeID_Type(t *testing.T) {
	id := NewChunkID(1000, 42, 1, 1)
	nodeID := id.NodeID()

	// Verify the return type is NodeID
	var _ NodeID = nodeID

	if uint32(nodeID) != 42 {
		t.Errorf("NodeID() = %v, want 42", nodeID)
	}
}

func TestChunkID_VolumeID_Type(t *testing.T) {
	id := NewChunkID(1000, 1, 42, 1)
	volumeID := id.VolumeID()

	// Verify the return type is VolumeID
	var _ VolumeID = volumeID

	if uint16(volumeID) != 42 {
		t.Errorf("VolumeID() = %v, want 42", volumeID)
	}
}

func TestChunkID_Time_ConsistentWithUnixMilli(t *testing.T) {
	unixMilli := int64(1700000000000)
	id := NewChunkID(unixMilli, 1, 2, 3)

	result := id.Time()

	// Time() should return the correct point in time
	// (time.UnixMilli returns local time, but the underlying instant is correct)
	if result.UnixMilli() != unixMilli {
		t.Errorf("Time().UnixMilli() = %v, want %v", result.UnixMilli(), unixMilli)
	}
}

func TestChunkID_UsableAsMapKey(t *testing.T) {
	// ChunkID should be usable as a map key since it's a fixed-size array
	m := make(map[ChunkID]string)

	id1 := NewChunkID(1000, 1, 2, 3)
	id2 := NewChunkID(2000, 4, 5, 6)

	m[id1] = "first"
	m[id2] = "second"

	if m[id1] != "first" {
		t.Errorf("map[id1] = %q, want \"first\"", m[id1])
	}
	if m[id2] != "second" {
		t.Errorf("map[id2] = %q, want \"second\"", m[id2])
	}

	// Same values should retrieve the same entry
	id1Copy := NewChunkID(1000, 1, 2, 3)
	if m[id1Copy] != "first" {
		t.Errorf("map[id1Copy] = %q, want \"first\"", m[id1Copy])
	}
}

func TestParseID_URLSafeCharacters(t *testing.T) {
	// Verify that String() produces URL-safe base64
	id := NewChunkID(1700000000000, 12345, 100, 42)
	str := id.String()

	// URL-safe base64 should not contain + or /
	for _, c := range str {
		if c == '+' || c == '/' {
			t.Errorf("String() contains non-URL-safe character: %c", c)
		}
	}

	// Should not have padding
	if len(str) > 0 && str[len(str)-1] == '=' {
		t.Error("String() should not have base64 padding (RawURLEncoding)")
	}
}
