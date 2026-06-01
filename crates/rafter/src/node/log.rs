use crate::{LogEntry, LogIndex, Term};

use super::{LocalProposalDropReason, Node};

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

    fn first_available_log_index(&self) -> LogIndex {
        self.snapshot_index().next()
    }

    pub(super) fn last_log_term(&self) -> Term {
        self.term_at(self.last_log_index()).unwrap_or_default()
    }

    /// Returns a clone of the log suffix starting at `first_index`.
    ///
    /// Raft log indexes are one-based. `LogIndex::ZERO` and indexes beyond the
    /// local tail return an empty suffix.
    #[must_use]
    pub fn log_entries_from(&self, first_index: LogIndex) -> Vec<LogEntry> {
        if first_index == LogIndex::ZERO || first_index > self.last_log_index() {
            return Vec::new();
        }

        let first_available = self.first_available_log_index();
        let first_index = std::cmp::max(first_index, first_available);
        let start = log_offset(first_index.0 - first_available.0);
        self.persistent.log[start..].to_vec()
    }

    pub(super) fn log_entries_from_bounded(
        &self,
        first_index: LogIndex,
        max_replication_bytes: usize,
    ) -> Vec<LogEntry> {
        if first_index == LogIndex::ZERO || first_index > self.last_log_index() {
            return Vec::new();
        }

        let first_available = self.first_available_log_index();
        let first_index = std::cmp::max(first_index, first_available);
        let start = log_offset(first_index.0 - first_available.0);
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

        entries
    }

    pub(super) fn entry_at(&self, index: LogIndex) -> Option<&LogEntry> {
        if index <= self.snapshot_index() {
            return None;
        }

        let offset = log_offset(index.0 - self.first_available_log_index().0);
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
        let dropped_proposals = self.volatile.local_proposals.split_off(&index);
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

        let first_available = self.first_available_log_index();
        if index <= first_available {
            self.persistent.log.clear();
            self.configuration_offsets.clear();
            return outputs;
        }

        let len = log_offset(index.0 - first_available.0);
        self.persistent.log.truncate(len);
        self.configuration_offsets.retain(|offset| *offset < len);
        outputs
    }

    /// Returns the installed local snapshot descriptor, if any.
    #[must_use]
    pub fn snapshot(&self) -> Option<&crate::RaftSnapshot> {
        self.persistent.snapshot.as_ref()
    }

    /// Installs a local snapshot descriptor and compacts covered log entries.
    pub fn install_local_snapshot(&mut self, snapshot: crate::RaftSnapshot) -> Vec<super::Output> {
        let committed_configuration =
            self.committed_configuration_state_at(snapshot.metadata.last_included_index);
        self.install_snapshot_state_with_committed_configuration(snapshot, committed_configuration)
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

fn log_offset(value: u64) -> usize {
    match usize::try_from(value) {
        Ok(offset) => offset,
        Err(_) => usize::MAX,
    }
}
