package port

import (
	"context"
	"time"

	"github.com/amari/mithril/chunk-node/domain"
)

// ChunkRepository defines the persistence interface for chunk metadata.
//
// This repository stores *only* metadata (TempChunk, AvailableChunk, DeletedChunk).
// It does NOT enforce domain rules, state transitions, or WriteKey/Version semantics.
// Those rules belong entirely in the service layer.
//
// The repository is responsible for:
//   - Looking up chunks by ID or WriteKey
//   - Persisting Temp, Available, and Deleted chunk records
//   - Returning union types (domain.Chunk) when the caller does not know the state
//   - Listing expired metadata for GC
//   - Removing metadata records
//
// The repository does NOT:
//   - Validate WriteKeys
//   - Validate Version numbers
//   - Enforce Temp→Available or Available→Deleted transitions
//   - Interact with the ChunkStore (physical storage)
//   - Perform any business logic
//
// This keeps the repository a pure persistence adapter.
type ChunkRepository_ interface {

	// -------------------------------------------------------------------------
	// State-specific lookups
	// -------------------------------------------------------------------------

	// GetTempByWriteKey returns the TempChunk associated with the given WriteKey.
	// Used by CreateChunk/PutChunk idempotency logic.
	GetTempByWriteKey(ctx context.Context, key []byte) (*domain.TempChunk, error)

	// GetAvailableByID returns an AvailableChunk by its ChunkID.
	// Used by AppendChunk, ReadChunk, StatChunk, DeleteChunk.
	GetAvailableByID(ctx context.Context, id domain.ChunkID) (*domain.AvailableChunk, error)

	// GetAvailableByWriteKey returns an AvailableChunk by WriteKey.
	// Useful when the client does not store ChunkID.
	GetAvailableByWriteKey(ctx context.Context, key []byte) (*domain.AvailableChunk, error)

	// GetDeletedByID returns a DeletedChunk by ChunkID.
	// Used by DeleteChunk retries and GC.
	GetDeletedByID(ctx context.Context, id domain.ChunkID) (*domain.DeletedChunk, error)

	// GetDeletedByWriteKey returns a DeletedChunk by WriteKey.
	// Useful for idempotent DeleteChunk calls.
	GetDeletedByWriteKey(ctx context.Context, key []byte) (*domain.DeletedChunk, error)

	// -------------------------------------------------------------------------
	// Union lookups (Temp, Available, or Deleted)
	// -------------------------------------------------------------------------

	// GetByWriteKey returns a TempChunk, AvailableChunk, or DeletedChunk.
	// This is used when the service layer does not yet know the state,
	// such as during CreateChunk/PutChunk idempotency handling.
	GetByWriteKey(ctx context.Context, key []byte) (domain.Chunk, error)

	// GetByID returns a TempChunk, AvailableChunk, or DeletedChunk by ChunkID.
	// Useful for GC or generic metadata inspection.
	GetByID(ctx context.Context, id domain.ChunkID) (domain.Chunk, error)

	// -------------------------------------------------------------------------
	// Writes (no domain semantics — pure persistence)
	// -------------------------------------------------------------------------

	// UpsertTemp inserts or replaces a TempChunk.
	// The service layer ensures that this operation is valid.
	UpsertTemp(ctx context.Context, chunk *domain.TempChunk) error

	// UpsertAvailable inserts or replaces an AvailableChunk.
	// The service layer ensures that this operation is valid.
	UpsertAvailable(ctx context.Context, chunk *domain.AvailableChunk) error

	// UpsertDeleted inserts or replaces a DeletedChunk.
	// Used by DeleteChunk and GC.
	UpsertDeleted(ctx context.Context, chunk *domain.DeletedChunk) error

	// -------------------------------------------------------------------------
	// Expiration queries (GC)
	// -------------------------------------------------------------------------

	// ListExpired returns TempChunks or DeletedChunks whose ExpiresAt <= now.
	// The returned slice may contain a mix of TempChunk and DeletedChunk values.
	// The service layer decides how to handle each expired entry.
	ListExpired(ctx context.Context, now time.Time, maxCount int) ([]domain.Chunk, error)

	// -------------------------------------------------------------------------
	// Final metadata removal (GC only)
	// -------------------------------------------------------------------------

	// RemoveByID permanently deletes metadata for the given ChunkID.
	// Used only by GC after physical deletion from the ChunkStore.
	RemoveByID(ctx context.Context, id domain.ChunkID) error
}
