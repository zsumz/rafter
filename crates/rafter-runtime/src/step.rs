//! The persist-before-output fence around one kernel step or batch.
//!
//! Every public way to drive the kernel funnels through one sequence here:
//! step the kernel, persist what changed, and only then release outputs. A
//! write that fails poisons the runtime instead of returning, because past
//! that point in-memory state describes a log the medium does not hold.

use rafter::{
    ClientProposalInput, Input as RaftInput, LocalProposalId, LogIndex, Node as RaftNode,
    Output as RaftOutput, ReadId, SnapshotChunkSource,
};
use rafter_storage::{
    BorrowedPersistedRaftLogEntry, RaftHardState, RaftHardStateStore, RaftLogSegment,
    RaftSnapshotStore,
};

use crate::hard_state::hard_state_for_node;
use crate::log_repair::{self, repair_persisted_log_suffix};
use crate::{DurableRaftNode, RaftRuntimeError, RaftRuntimeFatalError};

mod snapshot;

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
    /// effects before releasing outputs — group commit.
    ///
    /// Outputs from every batched input are withheld until persistence
    /// completes. A failed batch releases no outputs and poisons the runtime.
    /// Term and vote changes are fenced first, retaining the previous durable
    /// commit index and committed configuration. After snapshot and log work,
    /// a final hard-state write publishes the batch's new commit metadata.
    /// Newly accepted log entries across the whole batch land in one suffix
    /// append: a batch of proposals costs one log flush per batch. Staged
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
        // A durable suffix may still contain entries this step replaces.
        // Fence term/vote now, but preserve the previous committed prefix
        // until every required snapshot and log mutation is durable.
        let pre_log_hard_state = RaftHardState {
            current_term: current.current_term,
            voted_for: current.voted_for,
            ..persisted_before
        };

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

    pub(crate) fn poison(&mut self, error: RaftRuntimeError) -> RaftRuntimeError {
        if let Some(fatal_error) = RaftRuntimeFatalError::from_runtime_error(&error) {
            self.fatal_error = Some(fatal_error);
        }
        error
    }
}
