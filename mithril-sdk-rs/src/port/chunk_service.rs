use crate::domain::{Chunk, ChunkID};
use thiserror::Error;

//
// ────────────────────────────────────────────────────────────────
//  Chunk error types
// ────────────────────────────────────────────────────────────────
//

/// Errors that may occur when creating a new chunk.
///
/// A create operation initializes a brand‑new chunk with version `0` and size
/// determined by the service. Any failure here means the chunk was not created
/// or its final state is uncertain.
#[derive(Debug, Error)]
pub enum ChunkCreateError {
    /// The service could not be reached or the request failed in transit.
    #[error("network error")]
    Network,

    /// The service returned a chunk state with a version different from the
    /// expected initial version.
    #[error("version mismatch (expected {expected_version:?}, found {actual:?})")]
    VersionMismatch {
        expected_version: u64,
        actual: ActualChunkState,
    },

    /// The service encountered an I/O failure while creating the chunk.
    #[error("chunk i/o error")]
    Io { actual: ActualChunkState },

    /// The service encountered an unexpected internal error.
    #[error("internal error")]
    Internal { actual: Option<ActualChunkState> },
}

/// Errors for `put` operations are identical to `create` errors.
///
/// A `put` is effectively a create‑with‑payload operation.
pub type ChunkPutError = ChunkCreateError;

/// Errors that may occur when appending data to an existing chunk.
///
/// Append operations require the caller to provide the expected version of the
/// chunk. The service enforces strict version monotonicity.
#[derive(Debug, Error)]
pub enum ChunkAppendError {
    /// The service could not be reached or the request failed in transit.
    #[error("network error")]
    Network,

    /// The target chunk does not exist.
    #[error("chunk not found")]
    NotFound,

    /// The service returned a chunk state with a version different from the
    /// expected version supplied by the caller.
    #[error("chunk version mismatch (expected {expected_version:?}, found {actual:?})")]
    VersionMismatch {
        expected_version: u64,
        actual: ActualChunkState,
    },

    /// The service encountered an I/O failure while appending.
    #[error("chunk i/o error")]
    Io { actual: ActualChunkState },

    /// The service encountered an unexpected internal error.
    #[error("internal error")]
    Internal { actual: Option<ActualChunkState> },
}

/// Errors that may occur when reading from a chunk.
#[derive(Debug, Error)]
pub enum ChunkReadError {
    #[error("network error")]
    Network,

    #[error("chunk not found")]
    NotFound,

    #[error("chunk i/o error")]
    Io { actual: ActualChunkState },

    #[error("internal error")]
    Internal { actual: Option<ActualChunkState> },
}

/// Errors that may occur when retrieving metadata for a chunk.
#[derive(Debug, Error)]
pub enum ChunkStatError {
    #[error("network error")]
    Network,

    #[error("chunk not found")]
    NotFound,

    #[error("internal error")]
    Internal { actual: Option<ActualChunkState> },
}

/// Errors that may occur when retrieving deleting a chunk.
#[derive(Debug, Error)]
pub enum ChunkDeleteError {
    #[error("network error")]
    Network,

    #[error("chunk not found")]
    NotFound,

    #[error("internal error")]
    Internal,
}

/// Errors that may occurr when shrinking the tail slack of a chunk.
#[derive(Debug, Error)]
pub enum ChunkShrinkTailSlackError {
    #[error("network error")]
    Network,

    #[error("chunk not found")]
    NotFound,

    #[error("chunk version mismatch (expected {expected_version:?}, found {actual:?})")]
    VersionMismatch {
        expected_version: u64,
        actual: ActualChunkState,
    },

    #[error("chunk i/o error")]
    Io { actual: ActualChunkState },

    #[error("internal error")]
    Internal { actual: Option<ActualChunkState> },
}

//
// ────────────────────────────────────────────────────────────────
//  Chunk state
// ────────────────────────────────────────────────────────────────
//

/// The authoritative state of a chunk as returned by the service.
///
/// This includes the chunk's ID, its current version, and its logical size.
/// All write operations (`create`, `put`, `append`) return an updated
/// `ActualChunkState`.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub struct ActualChunkState {
    pub chunk_id: ChunkID,
    pub version: u64,
    pub size: i64,
}

//
// ────────────────────────────────────────────────────────────────
//  Chunk service interface
// ────────────────────────────────────────────────────────────────
//

/// Low‑level interface for interacting with chunk storage.
///
/// This trait defines the primitive operations used by higher‑level
/// orchestration layers. All operations return the updated chunk state on
/// success, and a structured error on failure.
pub trait ChunkService: Send + Sync {
    type CreateFut<'a>: Future<Output = Result<ActualChunkState, ChunkCreateError>> + Send + 'a;
    type PutFut<'a>: Future<Output = Result<ActualChunkState, ChunkPutError>> + Send + 'a;
    type AppendFut<'a>: Future<Output = Result<ActualChunkState, ChunkAppendError>> + Send + 'a;
    type ReadFut<'a>: Future<Output = Result<(usize, ActualChunkState), ChunkReadError>> + Send + 'a;
    type StatFut<'a>: Future<Output = Result<ActualChunkState, ChunkStatError>> + Send + 'a;
    type DeleteFut<'a>: Future<Output = Result<(), ChunkDeleteError>> + Send + 'a;
    type ShrinkTailSlackFut<'a>: Future<Output = Result<ActualChunkState, ChunkShrinkTailSlackError>>
        + Send
        + 'a;

    /// Create a new chunk with the given write key.
    fn create<'a>(&self, write_key: &'a [u8], min_tail_slack_length: i64) -> Self::CreateFut<'a>;

    /// Create a new chunk and write the provided payload into it.
    fn put<'a>(
        &self,
        write_key: &'a [u8],
        data: &'a [u8],
        min_tail_slack_length: i64,
    ) -> Self::PutFut<'a>;

    /// Append data to an existing chunk.
    fn append<'a>(
        &self,
        chunk: &'a Chunk,
        data: &'a [u8],
        min_tail_slack_length: i64,
    ) -> Self::AppendFut<'a>;

    /// Read data from a chunk into the provided buffer.
    fn read<'a>(&self, chunk: &'a Chunk, buf: &'a mut [u8]) -> Self::ReadFut<'a>;

    /// Retrieve the current state of a chunk without modifying it.
    fn stat<'a>(&self, chunk: &'a Chunk) -> Self::StatFut<'a>;

    /// Delete an existing chunk.
    fn delete<'a>(&self, chunk: &'a Chunk) -> Self::DeleteFut<'a>;

    /// Shrink the tail slack of an existing chunk.
    fn shrink_tail_slack<'a>(
        &self,
        chunk: &'a Chunk,
        max_tail_slack_size: u64,
    ) -> Self::ShrinkTailSlackFut<'a>;
}
