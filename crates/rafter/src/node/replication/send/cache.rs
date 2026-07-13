//! Per-broadcast cache for shared bounded log batches.

use crate::LogIndex;

use crate::node::{log::LogBatch, Node};

#[derive(Default)]
pub(super) struct LogBatchCache {
    batches: Vec<CachedLogBatch>,
}

struct CachedLogBatch {
    first_index: LogIndex,
    max_replication_bytes: usize,
    batch: LogBatch,
}

impl Node {
    pub(super) fn log_batch_from_bounded_cached(
        &self,
        first_index: LogIndex,
        max_replication_bytes: usize,
        batch_cache: &mut LogBatchCache,
    ) -> Option<LogBatch> {
        if let Some(cached) = batch_cache.batches.iter().find(|cached| {
            cached.first_index == first_index
                && cached.max_replication_bytes == max_replication_bytes
        }) {
            return Some(cached.batch.clone());
        }

        let batch = self.log_batch_from_bounded(first_index, max_replication_bytes)?;
        batch_cache.batches.push(CachedLogBatch {
            first_index,
            max_replication_bytes,
            batch: batch.clone(),
        });
        Some(batch)
    }
}
