use crate::service::executor::fragment::AppendFragmentExecutor;
use crate::service::plan::batch::{AppendFragmentBatchPlan, AppendFragmentBatchResult};
use core::future::Future;

/// Executes a batch of append operations for a single chunk.
///
/// All fragments must belong to the same chunk and are executed in order.
pub trait AppendFragmentBatchExecutor {
    type Fut<'a>: Future<Output = AppendFragmentBatchResult<'a>> + 'a
    where
        Self: 'a;

    fn execute<'a>(&'a self, plan: AppendFragmentBatchPlan<'a>) -> Self::Fut<'a>;
}

/// Default async implementation that executes each append sequentially.
pub struct DefaultAppendFragmentBatchExecutor<F: AppendFragmentExecutor> {
    pub fragment_executor: F,
}

impl<F: AppendFragmentExecutor + Sync> AppendFragmentBatchExecutor
    for DefaultAppendFragmentBatchExecutor<F>
{
    type Fut<'a>
        = impl Future<Output = AppendFragmentBatchResult<'a>> + Send + 'a
    where
        Self: 'a;

    fn execute<'a>(&'a self, plan: AppendFragmentBatchPlan<'a>) -> Self::Fut<'a> {
        async move {
            let mut results = Vec::with_capacity(plan.fragments.len());

            for frag_plan in plan.fragments {
                let r = self.fragment_executor.execute(frag_plan).await;
                results.push(r);
            }

            AppendFragmentBatchResult {
                chunk: plan.chunk,
                fragments: results,
            }
        }
    }
}
