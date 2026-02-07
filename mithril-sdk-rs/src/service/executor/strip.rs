use crate::service::executor::chunk::{DeleteChunkExecutor, StatChunkExecutor};
use crate::service::executor::fragment::{
    AppendFragmentExecutor, CreateFragmentExecutor, PutFragmentExecutor, ReadFragmentExecutor,
};
use crate::service::plan::strip::{
    AppendStripPlan, AppendStripResult, CreateStripPlan, CreateStripResult, DeleteStripPlan,
    DeleteStripResult, PutStripPlan, PutStripResult, ReadStripPlan, ReadStripResult, StatStripPlan,
    StatStripResult,
};
use core::future::Future;
use futures::future::join_all;

/// Executes all fragment‑creation operations for a strip.
pub trait CreateStripExecutor {
    type Fut<'a>: Future<Output = CreateStripResult> + Send + 'a
    where
        Self: 'a;

    fn execute<'a>(&'a self, plan: CreateStripPlan<'a>) -> Self::Fut<'a>;
}

/// Executes all fragment‑put operations for a strip.
pub trait PutStripExecutor {
    type Fut<'a>: Future<Output = PutStripResult> + Send + 'a
    where
        Self: 'a;

    fn execute<'a>(&'a self, plan: PutStripPlan<'a>) -> Self::Fut<'a>;
}

/// Executes all fragment‑append operations for a strip.
pub trait AppendStripExecutor {
    type Fut<'a>: Future<Output = AppendStripResult<'a>> + Send + 'a
    where
        Self: 'a;

    fn execute<'a>(&'a self, plan: AppendStripPlan<'a>) -> Self::Fut<'a>;
}

pub struct DefaultCreateStripExecutor<F: CreateFragmentExecutor> {
    pub fragment_executor: F,
}

impl<F: CreateFragmentExecutor + Sync> CreateStripExecutor for DefaultCreateStripExecutor<F> {
    type Fut<'a>
        = impl Future<Output = CreateStripResult> + Send + 'a
    where
        Self: 'a;

    fn execute<'a>(&'a self, plan: CreateStripPlan<'a>) -> Self::Fut<'a> {
        async move {
            let futs = plan
                .fragments
                .into_iter()
                .map(|p| self.fragment_executor.execute(p));

            let fragments = join_all(futs).await;

            CreateStripResult { fragments }
        }
    }
}

pub struct DefaultPutStripExecutor<F: PutFragmentExecutor> {
    pub fragment_executor: F,
}

impl<F: PutFragmentExecutor + Sync> PutStripExecutor for DefaultPutStripExecutor<F> {
    type Fut<'a>
        = impl Future<Output = PutStripResult> + Send + 'a
    where
        Self: 'a;

    fn execute<'a>(&'a self, plan: PutStripPlan<'a>) -> Self::Fut<'a> {
        async move {
            let futs = plan
                .fragments
                .into_iter()
                .map(|p| self.fragment_executor.execute(p));

            let fragments = join_all(futs).await;

            PutStripResult { fragments }
        }
    }
}

pub struct DefaultAppendStripExecutor<F: AppendFragmentExecutor> {
    pub fragment_executor: F,
}

impl<F: AppendFragmentExecutor + Sync> AppendStripExecutor for DefaultAppendStripExecutor<F> {
    type Fut<'a>
        = impl Future<Output = AppendStripResult<'a>> + Send + 'a
    where
        Self: 'a;

    fn execute<'a>(&'a self, plan: AppendStripPlan<'a>) -> Self::Fut<'a> {
        async move {
            let futs = plan
                .fragments
                .into_iter()
                .map(|p| self.fragment_executor.execute(p));

            let fragments = join_all(futs).await;

            AppendStripResult {
                strip: plan.strip,
                fragments,
            }
        }
    }
}

/// Executes all fragment reads in a strip concurrently.
pub trait ReadStripExecutor {
    type Fut<'a>: Future<Output = ReadStripResult<'a>> + Send + 'a
    where
        Self: 'a;

    fn execute<'a>(&'a self, plan: ReadStripPlan<'a>) -> Self::Fut<'a>;
}

/// Executes all chunk stats in a strip concurrently.
pub trait StatStripExecutor {
    type Fut<'a>: Future<Output = StatStripResult<'a>> + Send + 'a
    where
        Self: 'a;

    fn execute<'a>(&'a self, plan: StatStripPlan<'a>) -> Self::Fut<'a>;
}

pub struct DefaultReadStripExecutor<F: ReadFragmentExecutor> {
    pub fragment_executor: F,
}

impl<F: ReadFragmentExecutor + Sync> ReadStripExecutor for DefaultReadStripExecutor<F> {
    type Fut<'a>
        = impl Future<Output = ReadStripResult<'a>> + Send + 'a
    where
        Self: 'a;

    fn execute<'a>(&'a self, plan: ReadStripPlan<'a>) -> Self::Fut<'a> {
        async move {
            let futs = plan
                .fragments
                .into_iter()
                .map(|p| self.fragment_executor.execute(p));

            let fragments = join_all(futs).await;

            ReadStripResult {
                strip: plan.strip,
                fragments,
            }
        }
    }
}

pub struct DefaultStatStripExecutor<F: StatChunkExecutor> {
    pub fragment_executor: F,
}

impl<F: StatChunkExecutor + Sync> StatStripExecutor for DefaultStatStripExecutor<F> {
    type Fut<'a>
        = impl Future<Output = StatStripResult<'a>> + Send + 'a
    where
        Self: 'a;

    fn execute<'a>(&'a self, plan: StatStripPlan<'a>) -> Self::Fut<'a> {
        async move {
            let futs = plan
                .fragments
                .into_iter()
                .map(|p| self.fragment_executor.execute(p));

            let fragments = join_all(futs).await;

            StatStripResult {
                strip: plan.strip,
                fragments,
            }
        }
    }
}

/// Executes all chunk deletes in a strip concurrently.
pub trait DeleteStripExecutor {
    type Fut<'a>: Future<Output = DeleteStripResult<'a>> + Send + 'a
    where
        Self: 'a;

    fn execute<'a>(&'a self, plan: DeleteStripPlan<'a>) -> Self::Fut<'a>;
}

pub struct DefaultDeleteStripExecutor<F: DeleteChunkExecutor> {
    pub fragment_executor: F,
}

impl<F: DeleteChunkExecutor + Sync> DeleteStripExecutor for DefaultDeleteStripExecutor<F> {
    type Fut<'a>
        = impl Future<Output = DeleteStripResult<'a>> + Send + 'a
    where
        Self: 'a;

    fn execute<'a>(&'a self, plan: DeleteStripPlan<'a>) -> Self::Fut<'a> {
        async move {
            let futs = plan
                .fragments
                .into_iter()
                .map(|p| self.fragment_executor.execute(p));

            let fragments = join_all(futs).await;

            DeleteStripResult {
                strip: plan.strip,
                fragments,
            }
        }
    }
}
