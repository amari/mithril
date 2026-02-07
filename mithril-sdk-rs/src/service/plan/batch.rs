use crate::domain::Chunk;
use crate::service::plan::fragment::{AppendFragmentPlan, AppendFragmentResult};

/// A plan to append a batch of fragments belonging to the same chunk.
///
/// All fragments must target the same chunk and are executed in order.
pub struct AppendFragmentBatchPlan<'a> {
    pub chunk: &'a Chunk,
    pub fragments: Vec<AppendFragmentPlan<'a>>,
}

/// The result of executing an `AppendFragmentBatchPlan`.
pub struct AppendFragmentBatchResult<'a> {
    pub chunk: &'a Chunk,
    pub fragments: Vec<AppendFragmentResult<'a>>,
}
