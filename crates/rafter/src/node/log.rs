//! Retained-log access, mutation, compaction, and bounded batching.
//!
//! Every log mutation updates [`DerivedState`](super::state::DerivedState) in
//! the same transition so membership lookups never observe a stale index.

use std::{error::Error, fmt};

use crate::{LogEntry, LogIndex, SharedEntries, Term};

use super::state::LocalProposalTracker;
use super::{LocalProposalDropReason, Node, Output};

/// Why a caller-supplied local snapshot descriptor was not installed.
///
/// This enum is exhaustive because a local install is closed over the single
/// precondition it cannot verify from the descriptor alone.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalSnapshotInstallError {
    /// The descriptor's boundary lies beyond this node's committed prefix.
    ///
    /// Installing it would compact away entries no quorum has accepted and
    /// raise this node's commit index on the strength of a local call.
    BoundaryAheadOfCommit {
        snapshot_index: LogIndex,
        commit_index: LogIndex,
    },
}

impl fmt::Display for LocalSnapshotInstallError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self::BoundaryAheadOfCommit {
            snapshot_index,
            commit_index,
        } = self;
        write!(
            formatter,
            concat!(
                "local snapshot boundary {snapshot_index} lies beyond the committed ",
                "index {commit_index}"
            ),
            snapshot_index = snapshot_index,
            commit_index = commit_index,
        )
    }
}

impl Error for LocalSnapshotInstallError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct LogBatch {
    pub(super) first_index: LogIndex,
    pub(super) last_index: LogIndex,
    pub(super) entries: SharedEntries,
    pub(super) replication_bytes: usize,
}

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

    fn first_retained_log_index(&self) -> LogIndex {
        self.snapshot_index().next()
    }

    pub(super) fn last_log_term(&self) -> Term {
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

    pub(super) fn log_batch_from_bounded(
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

    pub(super) fn entry_at(&self, index: LogIndex) -> Option<&LogEntry> {
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

    pub(super) fn term_at(&self, index: LogIndex) -> Option<Term> {
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

    pub(super) fn truncate_from(
        &mut self,
        index: LogIndex,
        reason: LocalProposalDropReason,
    ) -> Vec<super::Output> {
        let dropped_proposals = self.volatile.local_proposals.split_off(index);
        let outputs = dropped_proposals
            .into_iter()
            .map(
                |(proposal_index, proposal)| super::Output::LocalProposalDropped {
                    proposal_id: proposal.id,
                    index: proposal_index,
                    term: proposal.term,
                    reason,
                },
            )
            .collect();

        let first_retained_index = self.first_retained_log_index();
        if index <= first_retained_index {
            self.persistent.log.clear();
            self.derived.configuration.clear();
            return outputs;
        }

        let retained_len = retained_log_offset(index.0 - first_retained_index.0);
        self.persistent.log.truncate(retained_len);
        self.derived.configuration.truncate(retained_len);
        outputs
    }

    /// Returns the installed local snapshot descriptor, if any.
    #[must_use]
    pub fn snapshot(&self) -> Option<&crate::RaftSnapshot> {
        self.persistent.snapshot.as_ref()
    }

    /// Installs a local snapshot descriptor and compacts covered log entries.
    ///
    /// This is the application-driven compaction path: an embedder that has
    /// built an application snapshot at its own applied index records the
    /// matching Raft descriptor here.
    /// `DurableRaftNode::compact_log_with_snapshot` in the `rafter-runtime`
    /// crate is the shipped caller, and persists the payload as well.
    ///
    /// # Precondition
    ///
    /// The boundary must lie at or below this node's commit index. A local
    /// descriptor carries no quorum evidence, so a boundary beyond the
    /// committed prefix would compact away entries that may still be
    /// overwritten, and this call would be manufacturing commitment from a
    /// local decision. The leader-sent install path is different in exactly
    /// that respect — a leader only snapshots committed state, so the
    /// descriptor it sends *is* commit evidence — and it keeps raising the
    /// commit index.
    ///
    /// Within the precondition the applied index is raised to the boundary
    /// too. That is not an extra assumption: the caller reached this method by
    /// building an application snapshot at the boundary, so the state machine
    /// has applied through it by construction.
    ///
    /// # Errors
    ///
    /// Returns [`LocalSnapshotInstallError::BoundaryAheadOfCommit`] when the
    /// descriptor's boundary lies beyond this node's committed prefix. Nothing
    /// is installed and no log entry is compacted.
    pub fn install_local_snapshot(
        &mut self,
        snapshot: crate::RaftSnapshot,
    ) -> Result<Vec<super::Output>, LocalSnapshotInstallError> {
        let snapshot_index = snapshot.metadata.last_included_index;
        let commit_index = self.volatile.commit_index;
        if snapshot_index > commit_index {
            return Err(LocalSnapshotInstallError::BoundaryAheadOfCommit {
                snapshot_index,
                commit_index,
            });
        }
        let committed_configuration = self.committed_configuration_state_at(snapshot_index);
        Ok(self
            .install_snapshot_state_with_committed_configuration(snapshot, committed_configuration))
    }

    pub(super) fn install_snapshot_state(
        &mut self,
        snapshot: crate::RaftSnapshot,
    ) -> Vec<super::Output> {
        let committed_configuration = snapshot.metadata.committed_configuration_state();
        self.install_snapshot_state_with_committed_configuration(snapshot, committed_configuration)
    }

    fn install_snapshot_state_with_committed_configuration(
        &mut self,
        snapshot: crate::RaftSnapshot,
        committed_configuration: Option<crate::CommittedConfiguration>,
    ) -> Vec<super::Output> {
        let boundary_index = snapshot.metadata.last_included_index;
        let boundary_term = snapshot.metadata.last_included_term;
        let retain_suffix = self.term_at(boundary_index) == Some(boundary_term);
        let retained_suffix = if retain_suffix {
            self.log_entries_from(boundary_index.next())
        } else {
            Vec::new()
        };

        self.persistent.snapshot = Some(snapshot);
        self.persistent.committed_configuration = committed_configuration;
        let outputs = self.replace_log(retained_suffix, LocalProposalDropReason::SnapshotCovered);
        if self.volatile.commit_index < boundary_index {
            self.volatile.commit_index = boundary_index;
        }
        if self.volatile.applied_index < boundary_index {
            self.volatile.applied_index = boundary_index;
        }
        outputs
    }
}

fn retained_log_offset(value: u64) -> usize {
    match usize::try_from(value) {
        Ok(offset) => offset,
        Err(_) => usize::MAX,
    }
}

impl Node {
    /// Appends one entry, keeping all derived log indexes exact.
    pub(super) fn append_log_entry(&mut self, entry: crate::LogEntry) {
        let offset = self.persistent.log.len();
        self.derived.configuration.record_append(offset, &entry);
        self.persistent.log.push(entry);
    }

    /// Replaces the whole log (bootstrap restores, splice rollbacks,
    /// snapshot installs) and rebuilds the offset index from it.
    pub(super) fn replace_log(
        &mut self,
        log: Vec<crate::LogEntry>,
        reason: LocalProposalDropReason,
    ) -> Vec<Output> {
        self.derived = super::state::DerivedState::from_log(&log);
        self.persistent.log = log;
        let mut retained = LocalProposalTracker::default();
        let mut outputs = Vec::new();
        let snapshot_index = self.snapshot_index();
        for (index, proposal) in std::mem::take(&mut self.volatile.local_proposals) {
            let covered_by_snapshot =
                reason == LocalProposalDropReason::SnapshotCovered && index <= snapshot_index;
            if !covered_by_snapshot && self.term_at(index) == Some(proposal.term) {
                retained.insert(index, proposal);
            } else {
                outputs.push(Output::LocalProposalDropped {
                    proposal_id: proposal.id,
                    index,
                    term: proposal.term,
                    reason,
                });
            }
        }
        self.volatile.local_proposals = retained;
        outputs
    }
}
