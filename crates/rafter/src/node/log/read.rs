//! Read-only views over the retained log and snapshot boundary.
//!
//! Nothing here mutates node state: these accessors answer index, term, and
//! suffix questions against the compacted prefix and retained entries, and
//! every mutation stays with the owning module in `node/log.rs`.

use crate::{LogEntry, LogIndex, Term};

use super::super::Node;

impl Node {
    /// Returns the index of the installed snapshot boundary, or zero.
    #[must_use]
    pub fn snapshot_index(&self) -> LogIndex {
        self.persistent
            .snapshot
            .as_ref()
            .map_or(LogIndex::ZERO, |snapshot| {
                snapshot.metadata.last_included_index
            })
    }

    pub(super) fn first_retained_log_index(&self) -> LogIndex {
        self.snapshot_index().next()
    }

    pub(in crate::node) fn last_log_term(&self) -> Term {
        self.term_at(self.last_log_index()).unwrap_or_default()
    }

    /// Returns a clone of the log suffix starting at `first_index`.
    ///
    /// Raft log indexes are one-based. `LogIndex::ZERO` and indexes beyond the
    /// local tail return an empty suffix. Use [`Node::log_entries_slice_from`]
    /// when the caller only needs to inspect or rematerialize the retained
    /// suffix without taking ownership of log entries.
    #[must_use]
    pub fn log_entries_from(&self, first_index: LogIndex) -> Vec<LogEntry> {
        self.log_entries_slice_from(first_index).to_vec()
    }

    /// Borrows the retained log suffix starting at `first_index`.
    ///
    /// Raft log indexes are one-based. `LogIndex::ZERO` and indexes beyond the
    /// local tail return an empty suffix. If `first_index` is below the local
    /// snapshot boundary, the returned slice starts at the first retained log
    /// entry after that boundary.
    #[must_use]
    pub fn log_entries_slice_from(&self, first_index: LogIndex) -> &[LogEntry] {
        if first_index == LogIndex::ZERO || first_index > self.last_log_index() {
            return &[];
        }

        let first_retained_index = self.first_retained_log_index();
        let first_index = std::cmp::max(first_index, first_retained_index);
        let start = retained_log_offset(first_index.0 - first_retained_index.0);
        &self.persistent.log[start..]
    }

    pub(in crate::node) fn entry_at(&self, index: LogIndex) -> Option<&LogEntry> {
        if index <= self.snapshot_index() {
            return None;
        }

        let offset = retained_log_offset(index.0 - self.first_retained_log_index().0);
        self.persistent.log.get(offset)
    }

    /// Returns the term at `index`, if the local log or snapshot boundary
    /// still contains it.
    #[must_use]
    pub fn term_at_index(&self, index: LogIndex) -> Option<Term> {
        self.term_at(index)
    }

    pub(in crate::node) fn term_at(&self, index: LogIndex) -> Option<Term> {
        let snapshot_index = self.snapshot_index();
        if index == LogIndex::ZERO && snapshot_index == LogIndex::ZERO {
            return Some(Term::default());
        }
        if index == snapshot_index {
            return self
                .persistent
                .snapshot
                .as_ref()
                .map(|snapshot| snapshot.metadata.last_included_term);
        }
        if index < snapshot_index {
            return None;
        }
        self.entry_at(index).map(|entry| entry.term)
    }
}

pub(super) fn retained_log_offset(value: u64) -> usize {
    match usize::try_from(value) {
        Ok(offset) => offset,
        Err(_) => usize::MAX,
    }
}
