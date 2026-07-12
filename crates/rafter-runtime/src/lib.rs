//! Durable runtime wrapper for the deterministic Rafter core.
//!
//! This crate persists hard state, log entries, snapshot staging, promoted
//! snapshots, and local compaction before returning Raft outputs to the
//! embedding. It owns persist-before-output sequencing for one durable Raft
//! node over the storage traits. It does not own application state,
//! state-machine apply idempotence, transport delivery, authenticated peer
//! identity, peer fencing, or application snapshot payload validation.
//! Datastore users should read the production boundary in the repository
//! README before treating the runtime as production glue.
//!
//! Prefer the `recover_with_storage_and_snapshot_store*` constructors on
//! restart paths so committed-but-unapplied recovery outputs are explicit and
//! can be applied before serving reads or accepting new writes.
//!
#[cfg(test)]
use rafter::BootstrapValidationError;
use rafter::{
    ClientProposalInput, CommittedConfiguration, ConfigurationEntry, Input as RaftInput,
    LocalProposalId, LogEntry, LogIndex, MembershipConfig, Message, Node as RaftNode,
    NodeId as RaftNodeId, Output as RaftOutput, RaftSnapshot, RaftSnapshotMetadata, ReadId,
    ReplicationProgress, Role as RaftRole, SnapshotChunkRequest, SnapshotChunkSource,
    SnapshotCommittedConfiguration, SnapshotTransferStatus, Term,
};
use rafter_storage::{
    BorrowedPersistedRaftLogEntry, InMemoryRaftHardStateStore, InMemoryRaftLogSegment,
    InMemoryRaftSnapshotStore, PersistedRaftSnapshot, RaftHardState, RaftHardStateStore,
    RaftLogSegment, RaftSnapshotStore,
};
#[cfg(test)]
use rafter_storage::{
    PersistedRaftLogEntry, RaftHardStateStoreWriteError, RaftLogSegmentAppendError,
    RaftLogSegmentCompactError, RaftLogSegmentTruncateError, RaftSnapshotStoreWriteError,
};

mod construction;
mod error;
mod log_repair;

pub use error::{RaftRuntimeError, RaftRuntimeFatalError};
use log_repair::repair_persisted_log_suffix;
pub use rafter_runtime_api::PersistedRaftRuntime;

/// Durable single-node runtime that persists Raft state before releasing
/// kernel outputs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableRaftNode<
    H = InMemoryRaftHardStateStore,
    L = InMemoryRaftLogSegment,
    S = InMemoryRaftSnapshotStore,
> {
    node: RaftNode,
    hard_state_store: H,
    log_segment: L,
    snapshot_store: S,
    /// Tail of the persisted log as of the last successful persist; lets
    /// suffix repair skip its divergence scan on the common no-conflict
    /// step. `None` forces one full scan, after which it is exact again.
    persisted_tail: Option<log_repair::PersistedTail>,
    fatal_error: Option<RaftRuntimeFatalError>,
}

/// Recovered durable runtime plus committed application outputs discovered
/// during recovery.
#[derive(Clone, Debug, Eq, PartialEq)]
#[must_use = "recovery outputs must be applied or intentionally discarded before using the node"]
pub struct RecoveredDurableRaftNode<
    H = InMemoryRaftHardStateStore,
    L = InMemoryRaftLogSegment,
    S = InMemoryRaftSnapshotStore,
> {
    node: DurableRaftNode<H, L, S>,
    recovery_outputs: Vec<RaftOutput>,
}

impl<H, L, S> RecoveredDurableRaftNode<H, L, S> {
    /// Splits the recovered runtime from the committed application outputs
    /// discovered during construction.
    ///
    /// Apply `recovery_outputs` before serving reads or accepting new writes
    /// from the recovered node unless the application has independently
    /// persisted those outputs.
    #[must_use]
    pub fn into_parts(self) -> (DurableRaftNode<H, L, S>, Vec<RaftOutput>) {
        (self.node, self.recovery_outputs)
    }
}

impl<H, L, S> DurableRaftNode<H, L, S> {
    /// Drains application outputs for committed log entries above the
    /// applied floor supplied at construction.
    ///
    /// Prefer
    /// [`DurableRaftNode::recover_with_storage_and_snapshot_store_applied_through`]
    /// for restart paths; it returns these outputs alongside the recovered
    /// node so callers cannot miss the recovery step accidentally. This
    /// method remains available for constructors that intentionally use the
    /// older two-step flow.
    #[must_use]
    pub fn drain_committed_outputs(&mut self) -> Vec<RaftOutput> {
        self.node.drain_committed_outputs()
    }
}

impl<H: RaftHardStateStore, L: RaftLogSegment, S: RaftSnapshotStore + SnapshotChunkSource>
    DurableRaftNode<H, L, S>
{
    /// Drives one deterministic Raft input and persists hard state, staged
    /// snapshot chunks, promoted snapshots, compaction, and newly accepted
    /// log entries before returning outputs.
    ///
    /// Leader-side [`RaftOutput::SendSnapshotChunk`] directives never reach
    /// the caller: after persistence, each one is resolved against the
    /// snapshot store into a [`RaftOutput::Send`] carrying the materialized
    /// [`rafter::InstallSnapshotChunk`] message. A directive the store cannot
    /// serve is silently dropped — equivalent to a lost message; the transfer
    /// resumes from the follower's acknowledged offset.
    ///
    /// # Errors
    ///
    /// Returns a persistence error when changed durable state cannot be written.
    /// After a fatal persistence failure the runtime remains poisoned until
    /// restart and suppresses all later outputs.
    pub fn step(&mut self, input: RaftInput) -> Result<Vec<RaftOutput>, RaftRuntimeError> {
        self.step_and_persist(|node| node.step(input))
    }

    /// Proposes an application payload with a local-only proposal ID.
    ///
    /// The returned outputs are released only after the same durable work as
    /// [`DurableRaftNode::step`], including the appended log entry when the
    /// proposal is accepted locally.
    ///
    /// # Errors
    ///
    /// Returns a persistence error when changed durable state cannot be
    /// written, or [`RaftRuntimeError::Poisoned`] after an earlier fatal
    /// runtime error.
    pub fn step_tracked_proposal(
        &mut self,
        proposal_id: LocalProposalId,
        payload: Vec<u8>,
    ) -> Result<Vec<RaftOutput>, RaftRuntimeError> {
        self.step(RaftInput::TrackedClientProposal {
            proposal_id,
            payload,
        })
    }

    /// Requests a typed read-index barrier.
    ///
    /// This delegates through [`DurableRaftNode::step`], so any outputs are
    /// released only after the runtime has satisfied its durability contract.
    ///
    /// # Errors
    ///
    /// Returns a persistence error when changed durable state cannot be
    /// written, or [`RaftRuntimeError::Poisoned`] after an earlier fatal
    /// runtime error.
    pub fn step_read_index(
        &mut self,
        read_id: ReadId,
    ) -> Result<Vec<RaftOutput>, RaftRuntimeError> {
        self.step(RaftInput::ReadIndex { read_id })
    }

    /// Drives one deterministic proposal batch and persists its combined
    /// effects before releasing outputs.
    ///
    /// This is the proposal-shaped sibling of [`DurableRaftNode::step_batch`]:
    /// it preserves the same all-or-nothing runtime durability fence while
    /// avoiding a generic input stream for the hot write path.
    ///
    /// # Errors
    ///
    /// Returns a persistence error when changed durable state cannot be
    /// written; the runtime is then poisoned until restart and no output from
    /// the failed batch is released.
    pub fn step_proposal_batch(
        &mut self,
        proposals: Vec<ClientProposalInput>,
    ) -> Result<Vec<RaftOutput>, RaftRuntimeError> {
        if proposals.is_empty() {
            return Ok(Vec::new());
        }
        self.step_and_persist(|node| node.step_proposal_batch(proposals))
    }

    /// Drives several deterministic Raft inputs and persists their combined
    /// effects with one durable flush per store — group commit.
    ///
    /// The persist-before-output contract holds for the batch as a whole:
    /// outputs from EVERY batched input are withheld until the single flush
    /// completes, so nothing observable ever precedes its own durability. A
    /// crash or persistence failure anywhere in the batch releases no output
    /// at all — peers and the application see either the whole batch's
    /// effects after they are durable, or none of them. Hard state is
    /// written once with the batch-final value, which is sound because hard
    /// state obligations are monotone: persisting the final term forbids
    /// every promise a lower term could have made, and a vote cannot change
    /// within a term. Newly accepted log entries across the whole batch land
    /// in one suffix append, which is the fsync amortization: a batch of
    /// proposals costs one log flush instead of one per proposal. Staged
    /// snapshot chunks and promotions persist in kernel output order, as in
    /// single-input steps.
    ///
    /// [`RaftOutput::SendSnapshotChunk`] directives are resolved exactly as
    /// [`DurableRaftNode::step`] resolves them.
    ///
    /// # Errors
    ///
    /// Returns a persistence error when changed durable state cannot be
    /// written; the runtime is then poisoned until restart and no output
    /// from the failed batch is released. A poisoned runtime's accessors
    /// may report in-memory state ahead of what was persisted — restart
    /// from durable storage is the only recovery.
    pub fn step_batch(
        &mut self,
        inputs: Vec<RaftInput>,
    ) -> Result<Vec<RaftOutput>, RaftRuntimeError> {
        if inputs.len() == 1 {
            return match inputs.into_iter().next() {
                Some(input) => self.step(input),
                None => unreachable!("len was checked"),
            };
        }
        self.step_and_persist(|node| node.step_batch(inputs))
    }

    fn step_and_persist(
        &mut self,
        step: impl FnOnce(&mut RaftNode) -> Vec<RaftOutput>,
    ) -> Result<Vec<RaftOutput>, RaftRuntimeError> {
        if let Some(cause) = &self.fatal_error {
            return Err(RaftRuntimeError::Poisoned {
                cause: cause.clone(),
            });
        }

        let persisted_before = self.hard_state_store.current();
        let commit_floor = self.node.commit_index();
        let outputs = step(&mut self.node);
        let current = hard_state_for_node(&self.node);
        let pre_log_hard_state =
            hard_state_for_node_capped_at(&self.node, durable_last_log_index(&self.log_segment));

        // The kernel steps in place: after a persistence failure the
        // runtime is poisoned and releases nothing, so in-memory state past
        // the durable state is unobservable — restart is the only exit.
        if pre_log_hard_state != persisted_before {
            if let Err(error) = self.hard_state_store.write_hard_state(pre_log_hard_state) {
                return Err(self.poison(RaftRuntimeError::HardStateWrite(error)));
            }
        }
        if let Err(error) = self.persist_snapshot_outputs_for_step(&outputs) {
            return Err(self.poison(error));
        }
        if let Err(error) = self.clear_abandoned_snapshot_staging_for_step() {
            return Err(self.poison(error));
        }
        if let Err(error) = self.persist_log_suffix_for_step(commit_floor) {
            return Err(self.poison(error));
        }
        if current != self.hard_state_store.current() {
            if let Err(error) = self.hard_state_store.write_hard_state(current) {
                return Err(self.poison(RaftRuntimeError::HardStateWrite(error)));
            }
        }

        Ok(self.resolve_snapshot_chunk_sends(outputs))
    }

    /// Returns the local Raft node ID.
    #[must_use]
    pub fn id(&self) -> RaftNodeId {
        self.node.id()
    }

    /// Returns the best-known leader for client redirection, if one is known.
    #[must_use]
    pub fn leader_hint(&self) -> Option<RaftNodeId> {
        self.node.leader_hint()
    }

    /// Returns the local node's current Raft role.
    #[must_use]
    pub fn role(&self) -> RaftRole {
        self.node.role()
    }

    /// Returns the local node's current term.
    #[must_use]
    pub fn current_term(&self) -> Term {
        self.node.current_term()
    }

    /// Whether a read requested now would use the active leader lease.
    #[must_use]
    pub fn read_lease_active(&self) -> bool {
        self.node.read_lease_active()
    }

    /// Returns the highest log index committed by the local Raft kernel.
    #[must_use]
    pub fn commit_index(&self) -> rafter::LogIndex {
        self.node.commit_index()
    }

    /// Returns the highest log index known to the local Raft kernel.
    #[must_use]
    pub fn last_log_index(&self) -> rafter::LogIndex {
        self.node.last_log_index()
    }

    /// The term recorded at `index`, when the local log or snapshot
    /// boundary still covers it — what an application needs to build
    /// snapshot metadata for [`DurableRaftNode::compact_log_with_snapshot`]
    /// at an arbitrary applied boundary.
    #[must_use]
    pub fn term_at_index(&self, index: rafter::LogIndex) -> Option<Term> {
        self.node.term_at_index(index)
    }

    /// Returns the installed snapshot boundary index.
    #[must_use]
    pub fn snapshot_index(&self) -> rafter::LogIndex {
        self.node.snapshot_index()
    }

    /// Returns the installed snapshot descriptor, if the local kernel has one.
    #[must_use]
    pub fn snapshot(&self) -> Option<&RaftSnapshot> {
        self.node.snapshot()
    }

    /// Returns the durable snapshot store backing this runtime.
    #[must_use]
    pub fn snapshot_store(&self) -> &S {
        &self.snapshot_store
    }

    /// Returns the current inbound snapshot transfer state.
    #[must_use]
    pub fn snapshot_transfer_status(&self) -> SnapshotTransferStatus {
        self.node.snapshot_transfer_status()
    }

    /// Returns per-follower replication progress when this node is leader.
    #[must_use]
    pub fn leader_replication_progress(&self) -> Vec<ReplicationProgress> {
        self.node.leader_replication_progress()
    }

    /// Returns the catch-up barrier for a learner promotion, if one is active.
    #[must_use]
    pub fn promotion_barrier(&self, learner_id: RaftNodeId) -> Option<rafter::PromotionBarrier> {
        self.node.promotion_barrier(learner_id)
    }

    /// Returns the committed membership configuration.
    #[must_use]
    pub fn committed_membership(&self) -> MembershipConfig {
        self.node.committed_membership()
    }

    /// Returns the committed configuration entry, if the log has committed one.
    #[must_use]
    pub fn committed_configuration_entry(&self) -> Option<ConfigurationEntry> {
        self.node.committed_configuration_entry()
    }

    /// Returns the committed configuration state, including joint-consensus
    /// metadata when applicable.
    #[must_use]
    pub fn committed_configuration_state(&self) -> Option<CommittedConfiguration> {
        self.node.committed_configuration_state()
    }

    /// Returns the effective membership used for current quorum decisions.
    #[must_use]
    pub fn effective_membership(&self) -> MembershipConfig {
        self.node.effective_membership()
    }

    /// Returns the effective configuration entry used by the local kernel.
    #[must_use]
    pub fn effective_configuration_entry(&self) -> Option<ConfigurationEntry> {
        self.node.effective_configuration_entry()
    }

    /// Returns the membership recorded at the installed snapshot boundary.
    #[must_use]
    pub fn snapshot_committed_membership(&self) -> Option<MembershipConfig> {
        self.node.snapshot_committed_membership()
    }

    /// Returns local log entries starting at `first_index`.
    #[must_use]
    pub fn log_entries_from(&self, first_index: LogIndex) -> Vec<LogEntry> {
        self.node.log_entries_from(first_index)
    }

    /// Returns the hard-state image corresponding to the current in-memory
    /// kernel state.
    #[must_use]
    pub fn hard_state(&self) -> RaftHardState {
        hard_state_for_node(&self.node)
    }

    /// Compacts the local durable Raft log through a durable snapshot boundary.
    ///
    /// This metadata-only convenience API records an empty application
    /// snapshot payload. Call [`Self::compact_log_with_snapshot`] when the
    /// application has a real snapshot payload to transfer to lagging
    /// followers.
    ///
    /// # Errors
    ///
    /// Returns [`RaftRuntimeError::SnapshotAheadOfCommit`] when the snapshot
    /// boundary is ahead of the local committed/applied Raft index, and
    /// [`RaftRuntimeError::LogCompact`] when storage rejects or cannot persist
    /// the local compaction.
    pub fn compact_log_through_snapshot(
        &mut self,
        snapshot: &RaftSnapshotMetadata,
    ) -> Result<(), RaftRuntimeError> {
        self.compact_log_with_snapshot(PersistedRaftSnapshot {
            metadata: snapshot.clone(),
            application_payload: Vec::new(),
        })
    }

    /// Installs a local application snapshot into the Raft node, persists the
    /// snapshot bytes, and compacts the durable Raft log through the snapshot
    /// boundary.
    ///
    /// This is the leader-side companion to the snapshot transfer protocol:
    /// after this succeeds, the kernel holds only the snapshot descriptor and
    /// replication streams the persisted payload from the snapshot store to
    /// followers whose `next_index` falls behind the compacted prefix.
    ///
    /// Snapshot receivers authorize the sender against the snapshot boundary
    /// membership recorded in Raft metadata. A leader added after an older
    /// snapshot boundary must not be served that older descriptor from a
    /// reusable snapshot store; compact or select a snapshot whose boundary
    /// membership includes the leader before it streams to followers.
    ///
    /// # Errors
    ///
    /// Returns [`RaftRuntimeError::SnapshotAheadOfCommit`] when the snapshot
    /// boundary is ahead of the local committed/applied Raft index,
    /// [`RaftRuntimeError::SnapshotBoundaryTermMismatch`] when the local log or
    /// current snapshot cannot prove the supplied boundary term,
    /// [`RaftRuntimeError::SnapshotMembershipMismatch`] when caller-provided
    /// committed membership metadata disagrees with the local boundary, and a
    /// fatal persistence error when the snapshot or compaction cannot be
    /// written.
    pub fn compact_log_with_snapshot(
        &mut self,
        mut snapshot: PersistedRaftSnapshot,
    ) -> Result<(), RaftRuntimeError> {
        if let Some(cause) = &self.fatal_error {
            return Err(RaftRuntimeError::Poisoned {
                cause: cause.clone(),
            });
        }

        self.validate_local_snapshot_boundary(&snapshot.metadata)?;
        self.normalize_local_snapshot_membership(&mut snapshot.metadata)?;

        let descriptor =
            RaftSnapshot::from_payload(snapshot.metadata.clone(), &snapshot.application_payload);
        if let Err(error) = self.write_snapshot_and_compact_log(snapshot) {
            return Err(self.poison(error));
        }
        let mut staged_node = self.node.clone();
        staged_node.install_local_snapshot(descriptor);
        self.node = staged_node;
        Ok(())
    }

    /// As [`Self::compact_log_with_snapshot`], but the payload is pulled
    /// from `source` in bounded chunks and never materialized whole — the
    /// compaction path for state machines whose snapshots exceed memory.
    ///
    /// # Errors
    ///
    /// As [`Self::compact_log_with_snapshot`], plus a snapshot write error
    /// when the source cannot serve the snapshot it describes.
    pub fn compact_log_with_streamed_snapshot(
        &mut self,
        mut snapshot: RaftSnapshot,
        source: &dyn SnapshotChunkSource,
    ) -> Result<(), RaftRuntimeError> {
        if let Some(cause) = &self.fatal_error {
            return Err(RaftRuntimeError::Poisoned {
                cause: cause.clone(),
            });
        }
        self.validate_local_snapshot_boundary(&snapshot.metadata)?;
        let source_snapshot = snapshot.clone();
        self.normalize_local_snapshot_membership(&mut snapshot.metadata)?;
        let source = OriginalSnapshotChunkSource {
            source,
            snapshot: &source_snapshot,
        };

        let boundary_index = snapshot.metadata.last_included_index;
        let written = self
            .snapshot_store
            .write_snapshot_from_source(&snapshot, &source)
            .map_err(RaftRuntimeError::SnapshotWrite)
            .and_then(|()| {
                self.log_segment
                    .compact_prefix_through(boundary_index)
                    .map_err(RaftRuntimeError::LogCompact)
            });
        if let Err(error) = written {
            return Err(self.poison(error));
        }
        let mut staged_node = self.node.clone();
        staged_node.install_local_snapshot(snapshot);
        self.node = staged_node;
        Ok(())
    }

    fn normalize_local_snapshot_membership(
        &self,
        metadata: &mut RaftSnapshotMetadata,
    ) -> Result<(), RaftRuntimeError> {
        let snapshot_index = metadata.last_included_index;
        let expected_membership = self.node.membership_at_index(snapshot_index);
        let expected_configuration = self.node.committed_configuration_state_at(snapshot_index);
        let expected = SnapshotCommittedConfiguration::new(
            expected_configuration,
            expected_membership.clone(),
        );
        match &metadata.committed_configuration {
            None => {
                metadata.committed_configuration = Some(expected);
                Ok(())
            }
            Some(actual) if actual.membership != expected_membership => {
                Err(RaftRuntimeError::SnapshotMembershipMismatch {
                    snapshot_index,
                    expected: Box::new(expected_membership),
                    actual: Box::new(actual.membership.clone()),
                })
            }
            Some(actual) if actual.configuration != expected_configuration => {
                Err(RaftRuntimeError::SnapshotCommittedConfigurationMismatch {
                    snapshot_index,
                    expected: expected_configuration,
                    actual: actual.configuration,
                })
            }
            Some(_) => Ok(()),
        }
    }

    fn validate_local_snapshot_boundary(
        &self,
        metadata: &RaftSnapshotMetadata,
    ) -> Result<(), RaftRuntimeError> {
        let snapshot_index = metadata.last_included_index;
        let commit_index = self.node.commit_index();
        if snapshot_index > commit_index {
            return Err(RaftRuntimeError::SnapshotAheadOfCommit {
                snapshot_index,
                commit_index,
            });
        }
        let local_term = self.node.term_at_index(snapshot_index);
        if local_term != Some(metadata.last_included_term) {
            return Err(RaftRuntimeError::SnapshotBoundaryTermMismatch {
                snapshot_index,
                snapshot_term: metadata.last_included_term,
                local_term,
            });
        }
        Ok(())
    }

    /// Persists snapshot effects in kernel output order: each staged chunk
    /// lands durably in the store's staging area, and each applied snapshot
    /// promotes the completed staging to the current snapshot before the
    /// durable log is compacted through its boundary.
    fn persist_snapshot_outputs_for_step(
        &mut self,
        outputs: &[RaftOutput],
    ) -> Result<(), RaftRuntimeError> {
        for output in outputs {
            match output {
                RaftOutput::StageSnapshotChunk { chunk } => self
                    .snapshot_store
                    .stage_snapshot_chunk(chunk)
                    .map_err(RaftRuntimeError::SnapshotWrite)?,
                RaftOutput::ApplySnapshot { snapshot } => {
                    self.snapshot_store
                        .promote_staged_snapshot(snapshot)
                        .map_err(RaftRuntimeError::SnapshotWrite)?;
                    self.log_segment
                        .compact_prefix_through(snapshot.metadata.last_included_index)
                        .map_err(RaftRuntimeError::LogCompact)?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Clears durable staging for a transfer the kernel no longer tracks:
    /// a transfer the kernel abandoned must not survive as staged bytes, or
    /// a restart would resume a transfer the protocol has moved past.
    fn clear_abandoned_snapshot_staging_for_step(&mut self) -> Result<(), RaftRuntimeError> {
        if self.node.pending_snapshot_transfer().is_none()
            && self
                .snapshot_store
                .current_pending_snapshot_transfer()
                .is_some()
        {
            self.snapshot_store
                .clear_pending_snapshot_transfer()
                .map_err(RaftRuntimeError::SnapshotWrite)?;
        }
        Ok(())
    }

    /// Materializes leader chunk directives into wire messages by reading
    /// payload bytes from the snapshot store; unresolvable directives are
    /// dropped like lost messages.
    fn resolve_snapshot_chunk_sends(&self, outputs: Vec<RaftOutput>) -> Vec<RaftOutput> {
        if !outputs
            .iter()
            .any(|output| matches!(output, RaftOutput::SendSnapshotChunk { .. }))
        {
            return outputs;
        }

        outputs
            .into_iter()
            .filter_map(|output| match output {
                RaftOutput::SendSnapshotChunk { to, chunk } => chunk
                    .resolve(&self.snapshot_store)
                    .map(|message| RaftOutput::Send {
                        to,
                        message: Message::InstallSnapshotChunk(message),
                    }),
                other => Some(other),
            })
            .collect()
    }

    fn write_snapshot_and_compact_log(
        &mut self,
        snapshot: PersistedRaftSnapshot,
    ) -> Result<(), RaftRuntimeError> {
        let boundary_index = snapshot.metadata.last_included_index;
        self.snapshot_store
            .write_snapshot(snapshot)
            .map_err(RaftRuntimeError::SnapshotWrite)?;
        self.log_segment
            .compact_prefix_through(boundary_index)
            .map_err(RaftRuntimeError::LogCompact)
    }

    fn persist_log_suffix_for_step(
        &mut self,
        commit_floor: LogIndex,
    ) -> Result<(), RaftRuntimeError> {
        repair_persisted_log_suffix(
            &mut self.log_segment,
            &self.node,
            self.persisted_tail,
            commit_floor,
        )?;
        let first_new_index = self.log_segment.next_index();
        // Appends label kernel entries with segment indexes, so the
        // segment's next appendable index must sit above the snapshot
        // boundary: the kernel's first appendable index is boundary + 1. A
        // segment still behind the boundary would stamp entries with wrong
        // indexes, acknowledge them, and lose them at the next reopen's
        // bootstrap filter. The open-time compaction repair makes this
        // unreachable; refuse loudly rather than mislabel if it is ever
        // bypassed.
        let snapshot_index = self.node.snapshot_index();
        if first_new_index <= snapshot_index {
            return Err(RaftRuntimeError::LogBehindSnapshotBoundary {
                segment_next_index: first_new_index,
                snapshot_index,
            });
        }
        let entries = self.node.log_entries_slice_from(first_new_index);
        if !entries.is_empty() {
            self.log_segment
                .append_entries_borrowed(entries.iter().enumerate().map(|(offset, entry)| {
                    BorrowedPersistedRaftLogEntry::new(
                        LogIndex(first_new_index.0 + offset as u64),
                        entry.term,
                        &entry.kind,
                    )
                }))
                .map_err(RaftRuntimeError::LogAppend)?;
        }
        self.persisted_tail = Some(log_repair::PersistedTail::of_node(&self.node));
        Ok(())
    }

    fn poison(&mut self, error: RaftRuntimeError) -> RaftRuntimeError {
        if let Some(fatal_error) = RaftRuntimeFatalError::from_runtime_error(&error) {
            self.fatal_error = Some(fatal_error);
        }
        error
    }
}

impl<H: RaftHardStateStore, L: RaftLogSegment, S: RaftSnapshotStore + SnapshotChunkSource>
    PersistedRaftRuntime for DurableRaftNode<H, L, S>
{
    type Error = RaftRuntimeError;

    fn id(&self) -> RaftNodeId {
        DurableRaftNode::id(self)
    }

    fn leader_hint(&self) -> Option<RaftNodeId> {
        DurableRaftNode::leader_hint(self)
    }

    fn role(&self) -> RaftRole {
        DurableRaftNode::role(self)
    }

    fn current_term(&self) -> Term {
        DurableRaftNode::current_term(self)
    }

    fn commit_index(&self) -> LogIndex {
        DurableRaftNode::commit_index(self)
    }

    fn last_log_index(&self) -> LogIndex {
        DurableRaftNode::last_log_index(self)
    }

    fn snapshot_index(&self) -> LogIndex {
        DurableRaftNode::snapshot_index(self)
    }

    fn membership(&self) -> MembershipConfig {
        DurableRaftNode::effective_membership(self)
    }

    fn committed_membership(&self) -> MembershipConfig {
        DurableRaftNode::committed_membership(self)
    }

    fn replication(&self) -> Vec<ReplicationProgress> {
        DurableRaftNode::leader_replication_progress(self)
    }

    fn step(&mut self, input: RaftInput) -> Result<Vec<RaftOutput>, RaftRuntimeError> {
        DurableRaftNode::step(self, input)
    }

    fn step_proposal_batch(
        &mut self,
        proposals: Vec<ClientProposalInput>,
    ) -> Result<Vec<RaftOutput>, RaftRuntimeError> {
        DurableRaftNode::step_proposal_batch(self, proposals)
    }

    fn step_batch(&mut self, inputs: Vec<RaftInput>) -> Result<Vec<RaftOutput>, RaftRuntimeError> {
        DurableRaftNode::step_batch(self, inputs)
    }

    fn term_at_index(&self, index: LogIndex) -> Option<Term> {
        DurableRaftNode::term_at_index(self, index)
    }
}

struct OriginalSnapshotChunkSource<'a> {
    source: &'a dyn SnapshotChunkSource,
    snapshot: &'a RaftSnapshot,
}

impl SnapshotChunkSource for OriginalSnapshotChunkSource<'_> {
    fn snapshot_chunk(&self, request: SnapshotChunkRequest<'_>) -> Option<Vec<u8>> {
        if request.total_payload_len != self.snapshot.application_payload_len
            || request.application_payload_crc32 != self.snapshot.application_payload_crc32
        {
            return None;
        }
        self.source.snapshot_chunk(SnapshotChunkRequest {
            transfer_id: self.snapshot.transfer_id(),
            metadata: &self.snapshot.metadata,
            total_payload_len: self.snapshot.application_payload_len,
            application_payload_crc32: self.snapshot.application_payload_crc32,
            offset: request.offset,
            len: request.len,
        })
    }
}

fn hard_state_for_node(node: &RaftNode) -> RaftHardState {
    hard_state_for_node_capped_at(node, node.commit_index())
}

fn hard_state_for_node_capped_at(node: &RaftNode, durable_commit_index: LogIndex) -> RaftHardState {
    let commit_index = node.commit_index().min(durable_commit_index);
    RaftHardState {
        current_term: node.current_term(),
        voted_for: node.voted_for(),
        commit_index,
        committed_configuration: node.committed_configuration_state_at(commit_index),
    }
}

fn durable_last_log_index<L: RaftLogSegment>(log_segment: &L) -> LogIndex {
    LogIndex(log_segment.next_index().0.saturating_sub(1))
}

#[cfg(test)]
mod tests;
