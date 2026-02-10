package domain

import (
	"testing"
	"time"
)

func TestTempChunk_ImplementsChunkInterface(t *testing.T) {
	var _ Chunk = &TempChunk{}
}

func TestAvailableChunk_ImplementsChunkInterface(t *testing.T) {
	var _ Chunk = &AvailableChunk{}
}

func TestDeletedChunk_ImplementsChunkInterface(t *testing.T) {
	var _ Chunk = &DeletedChunk{}
}

func TestTempChunk_ChunkID(t *testing.T) {
	id := NewChunkID(time.Now().UnixMilli(), 1, 2, 3)
	chunk := &TempChunk{ID: id}

	if chunk.ChunkID() != id {
		t.Errorf("ChunkID() = %v, want %v", chunk.ChunkID(), id)
	}
}

func TestAvailableChunk_ChunkID(t *testing.T) {
	id := NewChunkID(time.Now().UnixMilli(), 1, 2, 3)
	chunk := &AvailableChunk{ID: id}

	if chunk.ChunkID() != id {
		t.Errorf("ChunkID() = %v, want %v", chunk.ChunkID(), id)
	}
}

func TestDeletedChunk_ChunkID(t *testing.T) {
	id := NewChunkID(time.Now().UnixMilli(), 1, 2, 3)
	chunk := &DeletedChunk{ID: id}

	if chunk.ChunkID() != id {
		t.Errorf("ChunkID() = %v, want %v", chunk.ChunkID(), id)
	}
}

func TestTempChunk_ChunkWriterKey(t *testing.T) {
	writerKey := []byte("test-writer-key")
	chunk := &TempChunk{WriterKey: writerKey}

	if string(chunk.ChunkWriterKey()) != string(writerKey) {
		t.Errorf("ChunkWriterKey() = %v, want %v", chunk.ChunkWriterKey(), writerKey)
	}
}

func TestAvailableChunk_ChunkWriterKey(t *testing.T) {
	writerKey := []byte("test-writer-key")
	chunk := &AvailableChunk{WriterKey: writerKey}

	if string(chunk.ChunkWriterKey()) != string(writerKey) {
		t.Errorf("ChunkWriterKey() = %v, want %v", chunk.ChunkWriterKey(), writerKey)
	}
}

func TestDeletedChunk_ChunkWriterKey(t *testing.T) {
	writerKey := []byte("test-writer-key")
	chunk := &DeletedChunk{WriterKey: writerKey}

	if string(chunk.ChunkWriterKey()) != string(writerKey) {
		t.Errorf("ChunkWriterKey() = %v, want %v", chunk.ChunkWriterKey(), writerKey)
	}
}

func TestTempChunk_ChunkVersion(t *testing.T) {
	chunk := &TempChunk{}

	if chunk.ChunkVersion() != 0 {
		t.Errorf("ChunkVersion() = %v, want 0", chunk.ChunkVersion())
	}
}

func TestAvailableChunk_ChunkVersion(t *testing.T) {
	chunk := &AvailableChunk{Version: 42}

	if chunk.ChunkVersion() != 42 {
		t.Errorf("ChunkVersion() = %v, want 42", chunk.ChunkVersion())
	}
}

func TestDeletedChunk_ChunkVersion(t *testing.T) {
	chunk := &DeletedChunk{}

	if chunk.ChunkVersion() != 0 {
		t.Errorf("ChunkVersion() = %v, want 0", chunk.ChunkVersion())
	}
}

func TestTempChunk_ChunkSize(t *testing.T) {
	chunk := &TempChunk{}

	if chunk.ChunkSize() != 0 {
		t.Errorf("ChunkSize() = %v, want 0", chunk.ChunkSize())
	}
}

func TestAvailableChunk_ChunkSize(t *testing.T) {
	chunk := &AvailableChunk{Size: 1024}

	if chunk.ChunkSize() != 1024 {
		t.Errorf("ChunkSize() = %v, want 1024", chunk.ChunkSize())
	}
}

func TestDeletedChunk_ChunkSize(t *testing.T) {
	chunk := &DeletedChunk{}

	if chunk.ChunkSize() != 0 {
		t.Errorf("ChunkSize() = %v, want 0", chunk.ChunkSize())
	}
}

func TestTempChunk_AsTemp(t *testing.T) {
	chunk := &TempChunk{
		ID:        NewChunkID(time.Now().UnixMilli(), 1, 2, 3),
		WriterKey: []byte("key"),
	}

	result, ok := chunk.AsTemp()
	if !ok {
		t.Error("AsTemp() returned false, want true")
	}
	if result != chunk {
		t.Error("AsTemp() did not return the same chunk")
	}
}

func TestAvailableChunk_AsTemp(t *testing.T) {
	chunk := &AvailableChunk{}

	result, ok := chunk.AsTemp()
	if ok {
		t.Error("AsTemp() returned true, want false")
	}
	if result != nil {
		t.Error("AsTemp() returned non-nil, want nil")
	}
}

func TestDeletedChunk_AsTemp(t *testing.T) {
	chunk := &DeletedChunk{}

	result, ok := chunk.AsTemp()
	if ok {
		t.Error("AsTemp() returned true, want false")
	}
	if result != nil {
		t.Error("AsTemp() returned non-nil, want nil")
	}
}

func TestTempChunk_AsAvailable(t *testing.T) {
	chunk := &TempChunk{}

	result, ok := chunk.AsAvailable()
	if ok {
		t.Error("AsAvailable() returned true, want false")
	}
	if result != nil {
		t.Error("AsAvailable() returned non-nil, want nil")
	}
}

func TestAvailableChunk_AsAvailable(t *testing.T) {
	chunk := &AvailableChunk{
		ID:        NewChunkID(time.Now().UnixMilli(), 1, 2, 3),
		WriterKey: []byte("key"),
		Size:      1024,
		Version:   1,
	}

	result, ok := chunk.AsAvailable()
	if !ok {
		t.Error("AsAvailable() returned false, want true")
	}
	if result != chunk {
		t.Error("AsAvailable() did not return the same chunk")
	}
}

func TestDeletedChunk_AsAvailable(t *testing.T) {
	chunk := &DeletedChunk{}

	result, ok := chunk.AsAvailable()
	if ok {
		t.Error("AsAvailable() returned true, want false")
	}
	if result != nil {
		t.Error("AsAvailable() returned non-nil, want nil")
	}
}

func TestTempChunk_AsDeleted(t *testing.T) {
	chunk := &TempChunk{}

	result, ok := chunk.AsDeleted()
	if ok {
		t.Error("AsDeleted() returned true, want false")
	}
	if result != nil {
		t.Error("AsDeleted() returned non-nil, want nil")
	}
}

func TestAvailableChunk_AsDeleted(t *testing.T) {
	chunk := &AvailableChunk{}

	result, ok := chunk.AsDeleted()
	if ok {
		t.Error("AsDeleted() returned true, want false")
	}
	if result != nil {
		t.Error("AsDeleted() returned non-nil, want nil")
	}
}

func TestDeletedChunk_AsDeleted(t *testing.T) {
	chunk := &DeletedChunk{
		ID:        NewChunkID(time.Now().UnixMilli(), 1, 2, 3),
		WriterKey: []byte("key"),
	}

	result, ok := chunk.AsDeleted()
	if !ok {
		t.Error("AsDeleted() returned false, want true")
	}
	if result != chunk {
		t.Error("AsDeleted() did not return the same chunk")
	}
}

func TestTempChunk_IsTemp(t *testing.T) {
	chunk := &TempChunk{}

	if !chunk.IsTemp() {
		t.Error("IsTemp() = false, want true")
	}
}

func TestAvailableChunk_IsTemp(t *testing.T) {
	chunk := &AvailableChunk{}

	if chunk.IsTemp() {
		t.Error("IsTemp() = true, want false")
	}
}

func TestDeletedChunk_IsTemp(t *testing.T) {
	chunk := &DeletedChunk{}

	if chunk.IsTemp() {
		t.Error("IsTemp() = true, want false")
	}
}

func TestTempChunk_IsAvailable(t *testing.T) {
	chunk := &TempChunk{}

	if chunk.IsAvailable() {
		t.Error("IsAvailable() = true, want false")
	}
}

func TestAvailableChunk_IsAvailable(t *testing.T) {
	chunk := &AvailableChunk{}

	if !chunk.IsAvailable() {
		t.Error("IsAvailable() = false, want true")
	}
}

func TestDeletedChunk_IsAvailable(t *testing.T) {
	chunk := &DeletedChunk{}

	if chunk.IsAvailable() {
		t.Error("IsAvailable() = true, want false")
	}
}

func TestTempChunk_IsDeleted(t *testing.T) {
	chunk := &TempChunk{}

	if chunk.IsDeleted() {
		t.Error("IsDeleted() = true, want false")
	}
}

func TestAvailableChunk_IsDeleted(t *testing.T) {
	chunk := &AvailableChunk{}

	if chunk.IsDeleted() {
		t.Error("IsDeleted() = true, want false")
	}
}

func TestDeletedChunk_IsDeleted(t *testing.T) {
	chunk := &DeletedChunk{}

	if !chunk.IsDeleted() {
		t.Error("IsDeleted() = false, want true")
	}
}

func TestChunkInterface_TypeSwitch(t *testing.T) {
	now := time.Now()
	id := NewChunkID(now.UnixMilli(), 1, 2, 3)
	writerKey := []byte("test-key")

	chunks := []Chunk{
		&TempChunk{ID: id, WriterKey: writerKey, ExpiresAt: now.Add(time.Hour), CreatedAt: now},
		&AvailableChunk{ID: id, WriterKey: writerKey, Size: 1024, Version: 1, CreatedAt: now, UpdatedAt: now},
		&DeletedChunk{ID: id, WriterKey: writerKey, ExpiresAt: now.Add(time.Hour), DeletedAt: now},
	}

	expectedTypes := []string{"TempChunk", "AvailableChunk", "DeletedChunk"}

	for i, chunk := range chunks {
		var actualType string
		switch chunk.(type) {
		case *TempChunk:
			actualType = "TempChunk"
		case *AvailableChunk:
			actualType = "AvailableChunk"
		case *DeletedChunk:
			actualType = "DeletedChunk"
		default:
			t.Errorf("unexpected chunk type at index %d", i)
			continue
		}

		if actualType != expectedTypes[i] {
			t.Errorf("chunk[%d] type = %s, want %s", i, actualType, expectedTypes[i])
		}
	}
}

func TestChunkInterface_CommonAccessors(t *testing.T) {
	now := time.Now()
	id := NewChunkID(now.UnixMilli(), 1, 2, 3)
	writerKey := []byte("common-key")

	chunks := []Chunk{
		&TempChunk{ID: id, WriterKey: writerKey},
		&AvailableChunk{ID: id, WriterKey: writerKey, Size: 2048, Version: 5},
		&DeletedChunk{ID: id, WriterKey: writerKey},
	}

	for i, chunk := range chunks {
		if chunk.ChunkID() != id {
			t.Errorf("chunk[%d].ChunkID() = %v, want %v", i, chunk.ChunkID(), id)
		}
		if string(chunk.ChunkWriterKey()) != string(writerKey) {
			t.Errorf("chunk[%d].ChunkWriterKey() = %v, want %v", i, chunk.ChunkWriterKey(), writerKey)
		}
	}
}

func TestChunkStates_MutualExclusivity(t *testing.T) {
	testCases := []struct {
		name        string
		chunk       Chunk
		expectTemp  bool
		expectAvail bool
		expectDel   bool
	}{
		{
			name:        "TempChunk",
			chunk:       &TempChunk{},
			expectTemp:  true,
			expectAvail: false,
			expectDel:   false,
		},
		{
			name:        "AvailableChunk",
			chunk:       &AvailableChunk{},
			expectTemp:  false,
			expectAvail: true,
			expectDel:   false,
		},
		{
			name:        "DeletedChunk",
			chunk:       &DeletedChunk{},
			expectTemp:  false,
			expectAvail: false,
			expectDel:   true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.chunk.IsTemp() != tc.expectTemp {
				t.Errorf("IsTemp() = %v, want %v", tc.chunk.IsTemp(), tc.expectTemp)
			}
			if tc.chunk.IsAvailable() != tc.expectAvail {
				t.Errorf("IsAvailable() = %v, want %v", tc.chunk.IsAvailable(), tc.expectAvail)
			}
			if tc.chunk.IsDeleted() != tc.expectDel {
				t.Errorf("IsDeleted() = %v, want %v", tc.chunk.IsDeleted(), tc.expectDel)
			}

			// Verify exactly one state is true
			trueCount := 0
			if tc.chunk.IsTemp() {
				trueCount++
			}
			if tc.chunk.IsAvailable() {
				trueCount++
			}
			if tc.chunk.IsDeleted() {
				trueCount++
			}
			if trueCount != 1 {
				t.Errorf("expected exactly one state to be true, got %d", trueCount)
			}
		})
	}
}

func TestAsConversions_Consistency(t *testing.T) {
	testCases := []struct {
		name  string
		chunk Chunk
	}{
		{"TempChunk", &TempChunk{}},
		{"AvailableChunk", &AvailableChunk{}},
		{"DeletedChunk", &DeletedChunk{}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// AsTemp should be consistent with IsTemp
			_, okTemp := tc.chunk.AsTemp()
			if okTemp != tc.chunk.IsTemp() {
				t.Errorf("AsTemp() ok = %v, IsTemp() = %v, should be equal", okTemp, tc.chunk.IsTemp())
			}

			// AsAvailable should be consistent with IsAvailable
			_, okAvail := tc.chunk.AsAvailable()
			if okAvail != tc.chunk.IsAvailable() {
				t.Errorf("AsAvailable() ok = %v, IsAvailable() = %v, should be equal", okAvail, tc.chunk.IsAvailable())
			}

			// AsDeleted should be consistent with IsDeleted
			_, okDel := tc.chunk.AsDeleted()
			if okDel != tc.chunk.IsDeleted() {
				t.Errorf("AsDeleted() ok = %v, IsDeleted() = %v, should be equal", okDel, tc.chunk.IsDeleted())
			}
		})
	}
}

func TestTempChunk_Fields(t *testing.T) {
	now := time.Now()
	expiresAt := now.Add(time.Hour)
	id := NewChunkID(now.UnixMilli(), 100, 200, 300)
	writerKey := []byte("temp-writer-key")

	chunk := &TempChunk{
		ID:        id,
		WriterKey: writerKey,
		ExpiresAt: expiresAt,
		CreatedAt: now,
	}

	if chunk.ID != id {
		t.Errorf("ID = %v, want %v", chunk.ID, id)
	}
	if string(chunk.WriterKey) != string(writerKey) {
		t.Errorf("WriterKey = %v, want %v", chunk.WriterKey, writerKey)
	}
	if !chunk.ExpiresAt.Equal(expiresAt) {
		t.Errorf("ExpiresAt = %v, want %v", chunk.ExpiresAt, expiresAt)
	}
	if !chunk.CreatedAt.Equal(now) {
		t.Errorf("CreatedAt = %v, want %v", chunk.CreatedAt, now)
	}
}

func TestAvailableChunk_Fields(t *testing.T) {
	now := time.Now()
	updatedAt := now.Add(time.Minute)
	id := NewChunkID(now.UnixMilli(), 100, 200, 300)
	writerKey := []byte("available-writer-key")

	chunk := &AvailableChunk{
		ID:        id,
		WriterKey: writerKey,
		Size:      4096,
		Version:   10,
		CreatedAt: now,
		UpdatedAt: updatedAt,
	}

	if chunk.ID != id {
		t.Errorf("ID = %v, want %v", chunk.ID, id)
	}
	if string(chunk.WriterKey) != string(writerKey) {
		t.Errorf("WriterKey = %v, want %v", chunk.WriterKey, writerKey)
	}
	if chunk.Size != 4096 {
		t.Errorf("Size = %v, want 4096", chunk.Size)
	}
	if chunk.Version != 10 {
		t.Errorf("Version = %v, want 10", chunk.Version)
	}
	if !chunk.CreatedAt.Equal(now) {
		t.Errorf("CreatedAt = %v, want %v", chunk.CreatedAt, now)
	}
	if !chunk.UpdatedAt.Equal(updatedAt) {
		t.Errorf("UpdatedAt = %v, want %v", chunk.UpdatedAt, updatedAt)
	}
}

func TestDeletedChunk_Fields(t *testing.T) {
	now := time.Now()
	expiresAt := now.Add(24 * time.Hour)
	id := NewChunkID(now.UnixMilli(), 100, 200, 300)
	writerKey := []byte("deleted-writer-key")

	chunk := &DeletedChunk{
		ID:        id,
		WriterKey: writerKey,
		ExpiresAt: expiresAt,
		DeletedAt: now,
	}

	if chunk.ID != id {
		t.Errorf("ID = %v, want %v", chunk.ID, id)
	}
	if string(chunk.WriterKey) != string(writerKey) {
		t.Errorf("WriterKey = %v, want %v", chunk.WriterKey, writerKey)
	}
	if !chunk.ExpiresAt.Equal(expiresAt) {
		t.Errorf("ExpiresAt = %v, want %v", chunk.ExpiresAt, expiresAt)
	}
	if !chunk.DeletedAt.Equal(now) {
		t.Errorf("DeletedAt = %v, want %v", chunk.DeletedAt, now)
	}
}

func TestChunkWriterKey_NilKey(t *testing.T) {
	tempChunk := &TempChunk{WriterKey: nil}
	availChunk := &AvailableChunk{WriterKey: nil}
	delChunk := &DeletedChunk{WriterKey: nil}

	if tempChunk.ChunkWriterKey() != nil {
		t.Error("TempChunk.ChunkWriterKey() should return nil for nil WriterKey")
	}
	if availChunk.ChunkWriterKey() != nil {
		t.Error("AvailableChunk.ChunkWriterKey() should return nil for nil WriterKey")
	}
	if delChunk.ChunkWriterKey() != nil {
		t.Error("DeletedChunk.ChunkWriterKey() should return nil for nil WriterKey")
	}
}

func TestChunkWriterKey_EmptyKey(t *testing.T) {
	emptyKey := []byte{}

	tempChunk := &TempChunk{WriterKey: emptyKey}
	availChunk := &AvailableChunk{WriterKey: emptyKey}
	delChunk := &DeletedChunk{WriterKey: emptyKey}

	if len(tempChunk.ChunkWriterKey()) != 0 {
		t.Error("TempChunk.ChunkWriterKey() should return empty slice for empty WriterKey")
	}
	if len(availChunk.ChunkWriterKey()) != 0 {
		t.Error("AvailableChunk.ChunkWriterKey() should return empty slice for empty WriterKey")
	}
	if len(delChunk.ChunkWriterKey()) != 0 {
		t.Error("DeletedChunk.ChunkWriterKey() should return empty slice for empty WriterKey")
	}
}

func TestAvailableChunk_ZeroValues(t *testing.T) {
	chunk := &AvailableChunk{}

	if chunk.ChunkSize() != 0 {
		t.Errorf("ChunkSize() = %v, want 0 for zero-value struct", chunk.ChunkSize())
	}
	if chunk.ChunkVersion() != 0 {
		t.Errorf("ChunkVersion() = %v, want 0 for zero-value struct", chunk.ChunkVersion())
	}
}

func TestAvailableChunk_LargeValues(t *testing.T) {
	chunk := &AvailableChunk{
		Size:    1<<62 - 1, // Large but valid int64
		Version: 1<<64 - 1, // Max uint64
	}

	if chunk.ChunkSize() != 1<<62-1 {
		t.Errorf("ChunkSize() = %v, want %v", chunk.ChunkSize(), int64(1<<62-1))
	}
	if chunk.ChunkVersion() != 1<<64-1 {
		t.Errorf("ChunkVersion() = %v, want %v", chunk.ChunkVersion(), uint64(1<<64-1))
	}
}
