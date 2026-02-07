use crate::domain::Chunk;
use crate::port::chunk_service::{
    ActualChunkState, ChunkDeleteError, ChunkShrinkTailSlackError, ChunkStatError,
};

/// A plan to stat a chunk.
pub struct StatChunkPlan<'a> {
    pub chunk: &'a Chunk,
}

/// The result of executing a `StatChunkPlan`.
pub struct StatChunkResult<'a> {
    pub chunk: &'a Chunk,
    pub outcome: Result<ActualChunkState, ChunkStatError>,
}

/// A plan to delete a chunk.
pub struct DeleteChunkPlan<'a> {
    pub chunk: &'a Chunk,
}

/// The result of executing a `DeleteChunkPlan`.
pub struct DeleteChunkResult<'a> {
    pub chunk: &'a Chunk,
    pub outcome: Result<(), ChunkDeleteError>,
}

/// A plan to shrink a chunk's tail slack.
pub struct ShrinkTailSlackChunkPlan<'a> {
    pub chunk: &'a Chunk,
    pub max_tail_slack_size: u64,
}

/// The result of executing a `ShrinkTailSlackChunkPlan`.
pub struct ShrinkTailSlackChunkResult<'a> {
    pub chunk: &'a Chunk,
    pub outcome: Result<ActualChunkState, ChunkShrinkTailSlackError>,
}
