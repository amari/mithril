package chunk

import (
	"context"
	"time"

	"github.com/amari/mithril/chunk-node/domain"
)

// ChunkRepository provides CRUD operations for chunk metadata.
// It is a dumb storage layer that does not enforce domain logic or state transitions.
// The service layer is responsible for validating state transitions, checking versions,
// and enforcing business rules before calling repository methods.
//
// The repository stores chunks as a union type (domain.Chunk) which can be in one of
// three states: TempChunk, AvailableChunk, or DeletedChunk. The repository treats all
// states equally and allows arbitrary state transitions - it's the service layer's job
// to ensure these transitions are valid.
type ChunkRepository interface {
	// Get retrieves a chunk by its ChunkID.
	// The chunk can be in any state (Temp, Available, or Deleted).
	//
	// Returns ErrChunkNotFound if no chunk with this ID exists.
	Get(ctx context.Context, id domain.ChunkID) (domain.Chunk, error)

	// GetByWriterKey retrieves a chunk by its WriterKey.
	// The chunk can be in any state (Temp, Available, or Deleted).
	//
	// WriterKey is used as a capability token for authorization and idempotency.
	// It must be unique across all chunks in all states.
	//
	// Returns ErrChunkNotFound if no chunk with this WriterKey exists.
	GetByWriterKey(ctx context.Context, writerKey []byte) (domain.Chunk, error)

	// Store saves or updates a chunk in the repository.
	// If a chunk with the same ID already exists, it is completely replaced.
	// The chunk can be in any state (Temp, Available, or Deleted).
	//
	// The repository does not validate:
	//   - State transitions (e.g., Temp -> Available is valid)
	//   - Version increments (service layer handles optimistic locking)
	//   - Field constraints (service layer ensures invariants)
	//
	// The repository does enforce:
	//   - WriterKey uniqueness: Returns ErrChunkWriterKeyConflict if a different
	//     chunk already has this WriterKey.
	//   - Atomicity: The store operation succeeds or fails atomically.
	//
	// Returns ErrChunkWriterKeyConflict if the WriterKey is already used by a different chunk.
	Store(ctx context.Context, chunk domain.Chunk) error

	// Delete permanently removes a chunk's metadata from the repository.
	// This is typically called by garbage collection after a DeletedChunk's
	// tombstone has expired.
	//
	// Delete is idempotent - calling it on a non-existent chunk succeeds.
	//
	// Returns nil even if the chunk doesn't exist (idempotent deletion).
	Delete(ctx context.Context, id domain.ChunkID) error

	// ListExpired returns chunks whose ExpiresAt timestamp is less than or equal to now.
	// This is used by garbage collection to find:
	//   - TempChunks that have expired (stale write transactions)
	//   - DeletedChunks that have expired (tombstones ready for permanent deletion)
	//
	// AvailableChunks do not have an ExpiresAt field and will never be returned.
	//
	// The chunks are returned in arbitrary order. Implementations should use efficient
	// indexing on the ExpiresAt field.
	//
	// Parameters:
	//   - now: The cutoff time. Chunks with ExpiresAt <= now are returned.
	//   - limit: Maximum number of chunks to return. If limit <= 0, returns all matching chunks.
	//            Callers should use a reasonable limit to avoid memory issues.
	//
	// Returns an empty slice (not nil) if no expired chunks exist.
	ListExpired(ctx context.Context, now time.Time, limit int) ([]domain.Chunk, error)
}
