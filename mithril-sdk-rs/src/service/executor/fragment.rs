use crate::port::ChunkService;
use crate::service::plan::fragment::{
    AppendFragmentPlan, AppendFragmentResult, CreateFragmentPlan, CreateFragmentResult,
    PutFragmentPlan, PutFragmentResult, ReadFragmentPlan, ReadFragmentResult,
};
use core::future::Future;

/// Executes a single fragment‑creation operation.
pub trait CreateFragmentExecutor {
    type Fut<'a>: Future<Output = CreateFragmentResult> + Send + 'a
    where
        Self: 'a;

    fn execute<'a>(&'a self, plan: CreateFragmentPlan<'a>) -> Self::Fut<'a>;
}

/// Executes a single fragment‑put operation.
pub trait PutFragmentExecutor {
    type Fut<'a>: Future<Output = PutFragmentResult> + Send + 'a
    where
        Self: 'a;

    fn execute<'a>(&'a self, plan: PutFragmentPlan<'a>) -> Self::Fut<'a>;
}

/// Executes a single fragment‑append operation.
pub trait AppendFragmentExecutor {
    type Fut<'a>: Future<Output = AppendFragmentResult<'a>> + Send + 'a
    where
        Self: 'a;

    fn execute<'a>(&'a self, plan: AppendFragmentPlan<'a>) -> Self::Fut<'a>;
}

/// Default async implementation using a `ChunkService`.
pub struct DefaultCreateFragmentExecutor<S: ChunkService> {
    pub service: S,
}

impl<S: ChunkService> CreateFragmentExecutor for DefaultCreateFragmentExecutor<S> {
    type Fut<'a>
        = impl Future<Output = CreateFragmentResult> + Send + 'a
    where
        Self: 'a;

    fn execute<'a>(&'a self, plan: CreateFragmentPlan<'a>) -> Self::Fut<'a> {
        async move {
            let outcome = self
                .service
                .create(plan.write_key, plan.min_tail_slack_length)
                .await;

            CreateFragmentResult {
                role: plan.role,
                outcome,
            }
        }
    }
}

pub struct DefaultPutFragmentExecutor<S: ChunkService> {
    pub service: S,
}

impl<S: ChunkService> PutFragmentExecutor for DefaultPutFragmentExecutor<S> {
    type Fut<'a>
        = impl Future<Output = PutFragmentResult> + Send + 'a
    where
        Self: 'a;

    fn execute<'a>(&'a self, plan: PutFragmentPlan<'a>) -> Self::Fut<'a> {
        async move {
            let outcome = self
                .service
                .put(plan.write_key, plan.payload, plan.min_tail_slack_length)
                .await;

            PutFragmentResult {
                role: plan.role,
                outcome,
            }
        }
    }
}

pub struct DefaultAppendFragmentExecutor<S: ChunkService> {
    pub service: S,
}

impl<S: ChunkService> AppendFragmentExecutor for DefaultAppendFragmentExecutor<S> {
    type Fut<'a>
        = impl Future<Output = AppendFragmentResult<'a>> + Send + 'a
    where
        Self: 'a;

    fn execute<'a>(&'a self, plan: AppendFragmentPlan<'a>) -> Self::Fut<'a> {
        async move {
            let outcome = self
                .service
                .append(plan.chunk, plan.payload, plan.min_tail_slack_length)
                .await;

            AppendFragmentResult {
                chunk: plan.chunk,
                fragment: plan.fragment,
                outcome,
            }
        }
    }
}

/// Executes a single fragment read.
pub trait ReadFragmentExecutor {
    type Fut<'a>: Future<Output = ReadFragmentResult<'a>> + Send + 'a
    where
        Self: 'a;

    fn execute<'a>(&'a self, plan: ReadFragmentPlan<'a>) -> Self::Fut<'a>;
}

/// Default async implementation using a `ChunkService`.
pub struct DefaultReadFragmentExecutor<S: ChunkService + Send + Sync> {
    pub service: S,
}

impl<S: ChunkService + Send + Sync> ReadFragmentExecutor for DefaultReadFragmentExecutor<S> {
    type Fut<'a>
        = impl Future<Output = ReadFragmentResult<'a>> + Send + 'a
    where
        Self: 'a;

    fn execute<'a>(&'a self, plan: ReadFragmentPlan<'a>) -> Self::Fut<'a> {
        async move {
            let outcome = self.service.read(plan.chunk, plan.buffer).await;

            ReadFragmentResult {
                chunk: plan.chunk,
                fragment: plan.fragment,
                outcome,
            }
        }
    }
}
