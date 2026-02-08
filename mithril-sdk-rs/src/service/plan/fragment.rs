use crate::domain::{Chunk, Fragment, Role};
use crate::port::chunk_service::{
    ActualChunkState, ChunkAppendError, ChunkCreateError, ChunkPutError, ChunkReadError,
};

/// A plan to create a new fragment for a specific role.
pub struct CreateFragmentPlan<'a> {
    pub role: Role,
    pub write_key: &'a [u8],
    pub min_tail_slack_length: i64,
}

/// The result of executing a `CreateFragmentPlan`.
pub struct CreateFragmentResult {
    pub role: Role,
    pub outcome: Result<ActualChunkState, ChunkCreateError>,
}

/// A plan to create a new fragment and write an initial payload.
pub struct PutFragmentPlan<'a> {
    pub role: Role,
    pub write_key: &'a [u8],
    pub payload: &'a [u8],
    pub min_tail_slack_length: i64,
}

/// The result of executing a `PutFragmentPlan`.
pub struct PutFragmentResult {
    pub role: Role,
    pub outcome: Result<ActualChunkState, ChunkPutError>,
}

/// A plan to append data to an existing fragment.
pub struct AppendFragmentPlan<'a> {
    pub chunk: &'a Chunk,
    pub fragment: &'a Fragment,
    pub payload: &'a [u8],
    pub min_tail_slack_length: i64,
}

/// The result of executing an `AppendFragmentPlan`.
pub struct AppendFragmentResult<'a> {
    pub chunk: &'a Chunk,
    pub fragment: &'a Fragment,
    pub outcome: Result<ActualChunkState, ChunkAppendError>,
}

/// A plan to read data from an existing fragment.
pub struct ReadFragmentPlan<'a> {
    pub chunk: &'a Chunk,
    pub fragment: &'a Fragment,
    pub buffer: &'a mut [u8],
}

/// The result of executing a `ReadFragmentPlan`.
pub struct ReadFragmentResult<'a> {
    pub chunk: &'a Chunk,
    pub fragment: &'a Fragment,
    pub outcome: Result<(usize, ActualChunkState), ChunkReadError>,
}
