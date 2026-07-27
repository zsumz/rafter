//! Retained-log access, mutation, compaction, and bounded batching.
//!
//! Every log mutation updates [`DerivedState`](super::state::DerivedState) in
//! the same transition so membership lookups never observe a stale index.

use std::{error::Error, fmt};

use crate::{CommittedConfiguration, LogEntry, LogIndex, MembershipConfig, SharedEntries, Term};

use super::state::LocalProposalTracker;
use super::{LocalProposalDropReason, Node, Output};

/// Why a caller-supplied local snapshot descriptor was not installed.
///
/// Every variant is a refusal: the node is exactly as it was before the call,
/// nothing was compacted, and no output was emitted.
///
/// This enum is `#[non_exhaustive]`. It was exhaustive when a local install was
/// closed over one precondition; it is closed over six, and the set is the
/// list of facts a descriptor asserts that the local node can check for itself
/// — which grows as the descriptor carries more.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum LocalSnapshotInstallError {
    /// The boundary lies strictly below the installed snapshot's boundary.
    ///
    /// Installing it would rewind the compacted prefix and replace a newer
    /// descriptor with an older one. Nothing below the installed boundary is
    /// retained, so such a descriptor cannot even be checked against the local
    /// log — this is the refusal, not a term disagreement.
    BoundaryBelowInstalledSnapshot {
        snapshot_index: LogIndex,
        installed_index: LogIndex,
    },
    /// The descriptor's boundary lies beyond this node's committed prefix.
    ///
    /// Installing it would compact away entries no quorum has accepted and
    /// raise this node's commit index on the strength of a local call.
    BoundaryAheadOfCommit {
        snapshot_index: LogIndex,
        commit_index: LogIndex,
    },
    /// The boundary is committed, but this node has not applied through it.
    ///
    /// Kept distinct from [`Self::BoundaryAheadOfCommit`] because it is a
    /// different mistake: the entries exist and are committed, but this node
    /// has never handed them to a state machine, so raising the applied index
    /// to the boundary would skip them silently and forever. Reachable on a
    /// node recovered below its committed prefix — see
    /// [`Node::from_bootstrap_applied_through`](crate::Node::from_bootstrap_applied_through)
    /// and [`Node::drain_committed_outputs`](crate::Node::drain_committed_outputs).
    BoundaryAheadOfApplied {
        snapshot_index: LogIndex,
        applied_index: LogIndex,
    },
    /// The descriptor's boundary term disagrees with the local log.
    ///
    /// `local_term` is `None` when this node retains nothing at the boundary.
    /// On the leader-sent install path a term disagreement means the local
    /// suffix belongs to another history and is discarded; a *local* descriptor
    /// carries no such authority, so the same disagreement is caller error.
    BoundaryTermMismatch {
        snapshot_index: LogIndex,
        snapshot_term: Term,
        local_term: Option<Term>,
    },
    /// The descriptor records committed membership the local node does not
    /// derive at the boundary.
    ///
    /// The descriptor outlives the entries it compacts and becomes this node's
    /// membership of record below the boundary, so a disagreeing copy would
    /// redefine the voter set out of a local call.
    CommittedMembershipMismatch {
        snapshot_index: LogIndex,
        expected: Box<MembershipConfig>,
        actual: Box<MembershipConfig>,
    },
    /// The descriptor records a committed configuration identity the local node
    /// does not derive at the boundary.
    CommittedConfigurationMismatch {
        snapshot_index: LogIndex,
        expected: Option<CommittedConfiguration>,
        actual: Option<CommittedConfiguration>,
    },
}

impl fmt::Display for LocalSnapshotInstallError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BoundaryBelowInstalledSnapshot {
                snapshot_index,
                installed_index,
            } => write!(
                formatter,
                "local snapshot boundary {snapshot_index} lies below the installed snapshot boundary {installed_index}"
            ),
            Self::BoundaryAheadOfCommit {
                snapshot_index,
                commit_index,
            } => write!(
                formatter,
                "local snapshot boundary {snapshot_index} lies beyond the committed index {commit_index}"
            ),
            Self::BoundaryAheadOfApplied {
                snapshot_index,
                applied_index,
            } => write!(
                formatter,
                "local snapshot boundary {snapshot_index} lies beyond the applied index {applied_index}"
            ),
            Self::BoundaryTermMismatch {
                snapshot_index,
                snapshot_term,
                local_term: Some(local_term),
            } => write!(
                formatter,
                "local snapshot boundary {snapshot_index} records term {snapshot_term} but the local log holds term {local_term}"
            ),
            Self::BoundaryTermMismatch {
                snapshot_index,
                snapshot_term,
                local_term: None,
            } => write!(
                formatter,
                "local snapshot boundary {snapshot_index} records term {snapshot_term} but the local log retains nothing at that index"
            ),
            Self::CommittedMembershipMismatch {
                snapshot_index,
                expected,
                actual,
            } => write!(
                formatter,
                "local snapshot boundary {snapshot_index} records committed membership {actual:?} but the local committed membership is {expected:?}"
            ),
            Self::CommittedConfigurationMismatch {
                snapshot_index,
                expected,
                actual,
            } => write!(
                formatter,
                "local snapshot boundary {snapshot_index} records committed configuration {actual:?} but the local committed configuration is {expected:?}"
            ),
        }
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
    /// # Contract
    ///
    /// A local descriptor is a claim, not evidence. The leader-sent install
    /// path can act on a descriptor's word — a leader only snapshots committed
    /// state, so what it sends *is* commit evidence, and it may raise the
    /// commit index and discard a suffix whose term disagrees. Nothing about a
    /// local call carries that authority. So this method checks every claim the
    /// descriptor makes that the local node can check for itself, and refuses
    /// the whole call if any of them is false:
    ///
    /// 1. The boundary lies **at or above** the installed snapshot boundary.
    /// 2. The boundary lies **at or below the commit index** — otherwise the
    ///    call would manufacture commitment out of a local decision.
    /// 3. The boundary lies **at or below the applied index** — otherwise the
    ///    call would raise the applied index over committed entries this node
    ///    has never emitted, and they would never be emitted afterwards.
    /// 4. The boundary **term matches the local log** at that index — or, at
    ///    the installed boundary, the installed descriptor's own term.
    /// 5. The descriptor's committed configuration, **when it carries one**,
    ///    matches what this node derives at the boundary.
    ///
    /// Rule 3 subsumes rule 2, because the applied index never exceeds the
    /// commit index. They stay separate errors because they are separate
    /// mistakes, and a caller that hits the first has a safety problem while a
    /// caller that hits the second has a recovery-ordering one.
    ///
    /// # Idempotency at the installed boundary
    ///
    /// A boundary **equal** to the installed one is accepted and defined as a
    /// re-record: the descriptor is stored, and nothing else changes — no entry
    /// is compacted, no index moves, no output is emitted. Repeating a call
    /// with the same descriptor therefore leaves this node identical, which is
    /// what a retry after a partially-completed compaction needs. It is not a
    /// refusal because the caller may have real work left at that boundary that
    /// this node cannot see: a composition whose durable log is still behind an
    /// already-installed boundary repairs it through exactly this call. Rules 4
    /// and 5 still apply, so the boundary's term and committed configuration
    /// cannot be rewritten under it, and a boundary strictly *below* the
    /// installed one is refused — that one would rewind the compacted prefix.
    ///
    /// A descriptor with **no** committed configuration is accepted, and this
    /// node keeps deriving that state locally. The kernel does not synthesize
    /// the missing copy into the caller's descriptor: the descriptor is what a
    /// caller persists and streams, and rewriting it here would put this node's
    /// installed metadata out of step with the bytes stored under it. A
    /// composition that owns the snapshot store should fill the field in before
    /// calling — `rafter-runtime`'s compaction API does exactly that.
    ///
    /// Within the contract the applied index and commit index are already at or
    /// above the boundary, so neither moves; the log suffix above the boundary
    /// always survives, because rule 4 has proven it belongs to this history.
    /// The returned outputs report local proposals the retained log no longer
    /// backs, and are ordinarily empty.
    ///
    /// # Errors
    ///
    /// Returns the [`LocalSnapshotInstallError`] naming the first violated rule,
    /// in the order listed above. On any refusal **nothing is installed, no log
    /// entry is compacted, no index moves, and no output is emitted**.
    pub fn install_local_snapshot(
        &mut self,
        snapshot: crate::RaftSnapshot,
    ) -> Result<Vec<super::Output>, LocalSnapshotInstallError> {
        let committed_configuration = self.check_local_snapshot(&snapshot)?;
        Ok(self
            .install_snapshot_state_with_committed_configuration(snapshot, committed_configuration))
    }

    /// Checks every local-install precondition without mutating anything, and
    /// returns the committed configuration state the install should record.
    fn check_local_snapshot(
        &self,
        snapshot: &crate::RaftSnapshot,
    ) -> Result<Option<CommittedConfiguration>, LocalSnapshotInstallError> {
        let snapshot_index = snapshot.metadata.last_included_index;

        let installed_index = self.snapshot_index();
        if snapshot_index < installed_index {
            return Err(LocalSnapshotInstallError::BoundaryBelowInstalledSnapshot {
                snapshot_index,
                installed_index,
            });
        }
        let commit_index = self.volatile.commit_index;
        if snapshot_index > commit_index {
            return Err(LocalSnapshotInstallError::BoundaryAheadOfCommit {
                snapshot_index,
                commit_index,
            });
        }
        let applied_index = self.volatile.applied_index;
        if snapshot_index > applied_index {
            return Err(LocalSnapshotInstallError::BoundaryAheadOfApplied {
                snapshot_index,
                applied_index,
            });
        }
        let snapshot_term = snapshot.metadata.last_included_term;
        let local_term = self.term_at(snapshot_index);
        if local_term != Some(snapshot_term) {
            return Err(LocalSnapshotInstallError::BoundaryTermMismatch {
                snapshot_index,
                snapshot_term,
                local_term,
            });
        }

        let committed_configuration = self.committed_configuration_state_at(snapshot_index);
        if let Some(declared) = snapshot.metadata.committed_configuration.as_ref() {
            let expected_membership = self.membership_at_index(snapshot_index);
            if declared.membership != expected_membership {
                return Err(LocalSnapshotInstallError::CommittedMembershipMismatch {
                    snapshot_index,
                    expected: Box::new(expected_membership),
                    actual: Box::new(declared.membership.clone()),
                });
            }
            if declared.configuration != committed_configuration {
                return Err(LocalSnapshotInstallError::CommittedConfigurationMismatch {
                    snapshot_index,
                    expected: committed_configuration,
                    actual: declared.configuration,
                });
            }
        }
        Ok(committed_configuration)
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
