use crate::port::ChunkService;
use crate::service::plan::chunk::{
    DeleteChunkPlan, DeleteChunkResult, StatChunkPlan, StatChunkResult,
};
use core::future::Future;

/// Executes a single chunk stat.
pub trait StatChunkExecutor {
    type Fut<'a>: Future<Output = StatChunkResult<'a>> + Send + 'a
    where
        Self: 'a;

    fn execute<'a>(&'a self, plan: StatChunkPlan<'a>) -> Self::Fut<'a>;
}

pub struct DefaultStatChunkExecutor<S: ChunkService + Send + Sync> {
    pub service: S,
}

impl<S: ChunkService + Send + Sync> StatChunkExecutor for DefaultStatChunkExecutor<S> {
    type Fut<'a>
        = impl Future<Output = StatChunkResult<'a>> + Send + 'a
    where
        Self: 'a;

    fn execute<'a>(&'a self, plan: StatChunkPlan<'a>) -> Self::Fut<'a> {
        async move {
            let outcome = self.service.stat(plan.chunk).await;

            StatChunkResult {
                chunk: plan.chunk,
                outcome,
            }
        }
    }
}

/// Executes a single chunk delete.
pub trait DeleteChunkExecutor {
    type Fut<'a>: Future<Output = DeleteChunkResult<'a>> + Send + 'a
    where
        Self: 'a;

    fn execute<'a>(&'a self, plan: DeleteChunkPlan<'a>) -> Self::Fut<'a>;
}

pub struct DefaultDeleteChunkExecutor<S: ChunkService + Send + Sync> {
    pub service: S,
}

impl<S: ChunkService + Send + Sync> DeleteChunkExecutor for DefaultDeleteChunkExecutor<S> {
    type Fut<'a>
        = impl Future<Output = DeleteChunkResult<'a>> + Send + 'a
    where
        Self: 'a;

    fn execute<'a>(&'a self, plan: DeleteChunkPlan<'a>) -> Self::Fut<'a> {
        async move {
            let outcome = self.service.delete(plan.chunk).await;

            DeleteChunkResult {
                chunk: plan.chunk,
                outcome,
            }
        }
    }
}
