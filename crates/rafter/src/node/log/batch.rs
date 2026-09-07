//! Bounded log batching for replication sends.
//!
//! A batch is budgeted beyond its first entry so an oversized entry already
//! in the log can never stall replication; the send path owns pacing and
//! retry policy, not this module.

use crate::{LogIndex, SharedEntries};

use super::super::Node;
use super::retained_log_offset;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::node) struct LogBatch {
    pub(in crate::node) first_index: LogIndex,
    pub(in crate::node) last_index: LogIndex,
    pub(in crate::node) entries: SharedEntries,
    pub(in crate::node) replication_bytes: usize,
}

impl Node {
    pub(in crate::node) fn log_batch_from_bounded(
        &self,
        first_index: LogIndex,
        max_replication_bytes: usize,
    ) -> Option<LogBatch> {
        if first_index == LogIndex::ZERO || first_index > self.last_log_index() {
            return None;
        }

        let first_retained_index = self.first_retained_log_index();
        let first_index = std::cmp::max(first_index, first_retained_index);
        let start = retained_log_offset(first_index.0 - first_retained_index.0);
        let mut bytes = 0usize;
        let mut entries = Vec::new();

        for entry in &self.persistent.log[start..] {
            let entry_bytes = entry.replication_bytes();
            let next_bytes = bytes.saturating_add(entry_bytes);
            // The budget bounds the batch beyond its first entry: a single
            // entry may exceed it, otherwise an oversized entry already in
            // the log (spliced from a leader with a larger budget, or
            // hydrated from disk) would stall replication forever.
            if !entries.is_empty() && next_bytes > max_replication_bytes {
                break;
            }
            entries.push(entry.clone());
            bytes = next_bytes;
        }

        let last_index = LogIndex(first_index.0 + entries.len().checked_sub(1)? as u64);
        Some(LogBatch {
            first_index,
            last_index,
            entries: entries.into(),
            replication_bytes: bytes,
        })
    }
}
