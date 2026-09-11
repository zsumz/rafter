//! Retained-log access, mutation, compaction, and bounded batching.
//!
//! Every log mutation updates [`DerivedState`](super::state::DerivedState) in
//! the same transition so membership lookups never observe a stale index.

use crate::{CommittedConfiguration, LogIndex};

use super::state::LocalProposalTracker;
use super::{LocalProposalDropReason, Node, Output};

mod batch;
mod install_error;
mod read;

pub use install_error::LocalSnapshotInstallError;

pub(in crate::node) use batch::LogBatch;
use read::retained_log_offset;

impl Node {
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
    /// 3. The boundary lies **at or below the dispatch cursor** — otherwise the
    ///    call would raise the dispatch cursor over committed entries this node
    ///    has never emitted, and they would never be emitted afterwards.
    /// 4. The boundary **term matches the local log** at that index — or, at
    ///    the installed boundary, the installed descriptor's own term.
    /// 5. The descriptor's committed configuration, **when it carries one**,
    ///    matches what this node derives at the boundary.
    /// 6. The descriptor's **author is a replica** — voter or learner — of the
    ///    membership at the boundary.
    ///
    /// Rule 3 subsumes rule 2, because the dispatch cursor never exceeds the
    /// commit index. They stay separate errors because they are separate
    /// mistakes, and a caller that hits the first has a safety problem while a
    /// caller that hits the second has a recovery-ordering one.
    ///
    /// Rule 6 is the one rule here that is not about this node's history: it is
    /// what bootstrap validation and the leader-sent receive path both check,
    /// restated at the only entry point that used to skip it. Without it a
    /// caller could install a descriptor that is durable, compacts the log below
    /// it, and then refuses to hydrate at the next restart — a node that accepts
    /// its way into being unbootable. A learner authoring its own snapshot is
    /// the ordinary case and is accepted; what is refused is an author the
    /// boundary membership does not contain at all.
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
    /// Within the contract the dispatch cursor and commit index are already at or
    /// above the boundary, so neither moves; the log suffix above the boundary
    /// always survives, because rule 4 has proven it belongs to this history.
    /// The returned outputs report local proposals the retained log no longer
    /// backs, and are ordinarily empty.
    ///
    /// # A boundary is not a history, and no [`Output::ConfigurationCommitted`]
    ///
    /// A snapshot carries the committed configuration **at** its boundary and
    /// nothing about the configurations that committed and were superseded below
    /// it. This call therefore emits no
    /// [`Output::ConfigurationCommitted`](super::Output::ConfigurationCommitted),
    /// and neither does installing a snapshot received from a leader: those
    /// outputs come from the commit index crossing a configuration *entry*, and
    /// compaction is what removes the entries. The same is true of a replica
    /// that catches up by snapshot rather than by log.
    ///
    /// **A consumer that retires identities must not treat this as recoverable
    /// locally.** It is not: the configurations are gone from every copy this
    /// node can reach. What survives is the boundary configuration itself, which
    /// is enough to raise a consumer's high-water mark to the greatest identity
    /// the cluster had committed as of the boundary — and a managed driver does
    /// exactly that, because it observes the boundary configuration as an
    /// ordinary committed fact. What cannot survive is an identity admitted and
    /// removed entirely below the boundary.
    ///
    /// Two mechanisms cover that gap and neither is the kernel's: a consumer's
    /// own durable record of what *it* witnessed, and the deployment's monotonic
    /// `NodeId` allocator for what nothing witnessed. See [`crate::NodeId`].
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
        let dispatched_index = self.volatile.dispatched_index;
        if snapshot_index > dispatched_index {
            return Err(LocalSnapshotInstallError::BoundaryAheadOfApplied {
                snapshot_index,
                applied_index: dispatched_index,
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
        let boundary_membership = self.membership_at_index(snapshot_index);
        if let Some(declared) = snapshot.metadata.committed_configuration.as_ref() {
            if declared.membership != boundary_membership {
                return Err(LocalSnapshotInstallError::CommittedMembershipMismatch {
                    snapshot_index,
                    expected: Box::new(boundary_membership),
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

        // Last, and against the *derived* membership: rule 5 has already proven
        // any declared copy equal to it, and a descriptor that declares none is
        // still judged against the boundary the runtime would have stamped in.
        // A membership disagreement is the more fundamental mistake, so it is
        // reported first.
        let writer_id = snapshot.metadata.writer_id;
        if !boundary_membership.contains_replica(writer_id) {
            return Err(LocalSnapshotInstallError::WriterNotBoundaryReplica {
                snapshot_index,
                writer_id,
                membership: Box::new(boundary_membership),
            });
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
        let outputs = self.replace_log(retained_suffix);
        if self.volatile.commit_index < boundary_index {
            self.volatile.commit_index = boundary_index;
        }
        if self.volatile.dispatched_index < boundary_index {
            self.volatile.dispatched_index = boundary_index;
        }
        outputs
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
    ///
    /// A tracked proposal survives only when the replacement keeps its term at
    /// the same index; every divergence is a log overwrite.
    pub(super) fn replace_log(&mut self, log: Vec<crate::LogEntry>) -> Vec<Output> {
        self.derived = super::state::DerivedState::from_log(&log);
        self.persistent.log = log;
        let mut retained = LocalProposalTracker::default();
        let mut outputs = Vec::new();
        for (index, proposal) in std::mem::take(&mut self.volatile.local_proposals) {
            if self.term_at(index) == Some(proposal.term) {
                retained.insert(index, proposal);
            } else {
                outputs.push(Output::LocalProposalDropped {
                    proposal_id: proposal.id,
                    index,
                    term: proposal.term,
                    reason: LocalProposalDropReason::LogOverwritten,
                });
            }
        }
        self.volatile.local_proposals = retained;
        outputs
    }
}
