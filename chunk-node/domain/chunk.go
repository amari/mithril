package domain

import "time"

// Chunk is a union type representing one of TempChunk,
// AvailableChunk, or DeletedChunk.
type Chunk interface {
	isChunk()

	// Common accessors across all states
	ChunkID() ChunkID
	ChunkWriterKey() []byte

	AsTemp() (*TempChunk, bool)
	AsAvailable() (*AvailableChunk, bool)
	AsDeleted() (*DeletedChunk, bool)

	IsTemp() bool
	IsAvailable() bool
	IsDeleted() bool
}

func (t *TempChunk) isChunk()      {}
func (a *AvailableChunk) isChunk() {}
func (d *DeletedChunk) isChunk()   {}

func (t *TempChunk) ChunkID() ChunkID      { return t.ID }
func (a *AvailableChunk) ChunkID() ChunkID { return a.ID }
func (d *DeletedChunk) ChunkID() ChunkID   { return d.ID }

func (t *TempChunk) ChunkWriterKey() []byte      { return t.WriterKey }
func (a *AvailableChunk) ChunkWriterKey() []byte { return a.WriterKey }
func (d *DeletedChunk) ChunkWriterKey() []byte   { return d.WriterKey }

func (t *TempChunk) AsTemp() (*TempChunk, bool)      { return t, true }
func (a *AvailableChunk) AsTemp() (*TempChunk, bool) { return nil, false }
func (d *DeletedChunk) AsTemp() (*TempChunk, bool)   { return nil, false }

func (t *TempChunk) AsAvailable() (*AvailableChunk, bool) { return nil, false }
func (a *AvailableChunk) AsAvailable() (*AvailableChunk, bool) {
	return a, true
}
func (d *DeletedChunk) AsAvailable() (*AvailableChunk, bool) { return nil, false }

func (t *TempChunk) AsDeleted() (*DeletedChunk, bool) { return nil, false }
func (a *AvailableChunk) AsDeleted() (*DeletedChunk, bool) {
	return nil, false
}
func (d *DeletedChunk) AsDeleted() (*DeletedChunk, bool) { return d, true }

func (t *TempChunk) IsTemp() bool      { return true }
func (a *AvailableChunk) IsTemp() bool { return false }
func (d *DeletedChunk) IsTemp() bool   { return false }

func (t *TempChunk) IsAvailable() bool      { return false }
func (a *AvailableChunk) IsAvailable() bool { return true }
func (d *DeletedChunk) IsAvailable() bool   { return false }

func (t *TempChunk) IsDeleted() bool      { return false }
func (a *AvailableChunk) IsDeleted() bool { return false }
func (d *DeletedChunk) IsDeleted() bool   { return true }

// TempChunk represents an in‑flight write transaction created by
// CreateChunk or PutChunk. It exists *only* to support idempotency
// and exclusive write authorization via WriteKey.
//
// A TempChunk does NOT represent a partially written chunk.
// It is simply a record that a client has begun a write operation
// using a specific WriteKey.
//
// Invariants:
//   - WriteKey is required and unique.
//   - The chunk is not readable or appendable.
//   - The chunk has not yet been committed to Available.
//   - ExpiresAt determines when the service layer should GC this record.
//   - CreatedAt is when the write transaction began.
type TempChunk struct {
	// ID is the chunk identifier chosen by the scheduler.
	// The underlying ChunkStore may or may not have allocated storage yet.
	ID ChunkID

	// WriterKey is the client‑supplied capability token.
	// It uniquely identifies this write transaction and authorizes writes.
	WriterKey []byte

	// ExpiresAt indicates when the service layer should treat this
	// TempChunk as stale and eligible for cleanup.
	ExpiresAt time.Time

	// CreatedAt is when the TempChunk record was created.
	CreatedAt time.Time
}

// AvailableChunk represents a fully committed, readable, appendable chunk.
// This is the "normal" state of a chunk after CreateChunk or PutChunk succeeds.
//
// Invariants:
//   - WriteKey is still required for AppendChunk, DeleteChunk, and shrink ops.
//   - Size is the logical size of the chunk in bytes.
//   - Version increments on every successful append.
//   - CreatedAt is the commit time (promotion from Temp).
//   - UpdatedAt is the last successful append or metadata update.
type AvailableChunk struct {
	// ID uniquely identifies the chunk and determines its physical placement.
	ID ChunkID

	// WriterKey is retained because clients may use it as the primary identifier
	// instead of ChunkID, and because it authorizes further writes.
	WriterKey []byte

	// Size is the logical size of the chunk in bytes.
	// It increases with each successful AppendChunk.
	Size int64

	// Version is incremented on each append and used for optimistic concurrency.
	Version uint64

	// CreatedAt is when the chunk was promoted from Temp to Available.
	CreatedAt time.Time

	// UpdatedAt is when the chunk was last modified (append or shrink).
	UpdatedAt time.Time
}

// DeletedChunk is a tombstone representing a chunk that has been logically
// deleted but not yet physically removed. It exists so that retries of
// DeleteChunk or StatChunk using WriteKey or ChunkID behave correctly.
//
// Invariants:
//   - WriteKey is retained so clients can still reference the chunk
//     during retries or for idempotent delete operations.
//   - ExpiresAt determines when GC should permanently remove metadata
//     and instruct the ChunkStore to delete the physical data.
//   - DeletedAt records when the chunk was marked deleted.
type DeletedChunk struct {
	// ID is the identifier of the deleted chunk.
	ID ChunkID

	// WriterKey is retained for idempotency and lookup even after deletion.
	WriterKey []byte

	// ExpiresAt indicates when the tombstone should be garbage‑collected.
	ExpiresAt time.Time

	// DeletedAt is when the chunk was marked as deleted.
	DeletedAt time.Time
}
