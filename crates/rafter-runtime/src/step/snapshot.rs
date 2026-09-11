//! The snapshot half of one step's durable work.
//!
//! Staged chunks, promotions, and the compaction that follows a promotion are
//! carried out here in kernel output order; abandoned staging is cleared; and
//! leader chunk directives are materialized from the store after the fence.
//! Nothing here decides output order — it rewrites one variant and no more.

use rafter::{Message, Output as RaftOutput, SnapshotChunkSource};
use rafter_storage::{RaftHardStateStore, RaftLogSegment, RaftSnapshotStore};

use crate::{DurableRaftNode, RaftRuntimeError};

impl<H: RaftHardStateStore, L: RaftLogSegment, S: RaftSnapshotStore + SnapshotChunkSource>
    DurableRaftNode<H, L, S>
{
    /// Persists snapshot effects in kernel output order: each staged chunk
    /// lands durably in the store's staging area, and each applied snapshot
    /// reconciles the durable boundary before promotion and prefix compaction.
    ///
    /// Deliberately without a wildcard arm. Everything else this step emits is
    /// released behind the ordinary persistence fence — hard state, log suffix,
    /// final hard state — and a new output that needed a store write of its own
    /// would otherwise get none, silently, with no compile error to say so.
    /// `persistence_contract::runtime_output_persistence_dependency` classifies
    /// every variant against that fence; this match is where the two that need
    /// more than the fence are carried out.
    pub(super) fn persist_snapshot_outputs_for_step(
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
                    crate::snapshot_install::install_staged_snapshot(
                        &mut self.log_segment,
                        &mut self.snapshot_store,
                        snapshot,
                        self.hard_state_store.current().commit_index,
                    )?;
                }
                // The remaining log and final hard-state writes make a
                // committed configuration durable before outputs escape.
                // This snapshot phase does not persist another copy.
                RaftOutput::ConfigurationCommitted { .. }
                | RaftOutput::SendSnapshotChunk { .. }
                | RaftOutput::LocalProposalAppended { .. }
                | RaftOutput::LocalProposalDropped { .. }
                | RaftOutput::Apply { .. }
                | RaftOutput::RejectProposal { .. }
                | RaftOutput::LeadershipTransferRejected { .. }
                | RaftOutput::ReadIndexGranted { .. }
                | RaftOutput::ReadIndexRejected { .. }
                | RaftOutput::ReadIndexCanceled { .. }
                | RaftOutput::Send { .. } => {}
            }
        }
        Ok(())
    }

    /// Clears durable staging for a transfer the kernel no longer tracks:
    /// a transfer the kernel abandoned must not survive as staged bytes, or
    /// a restart would resume a transfer the protocol has moved past.
    pub(super) fn clear_abandoned_snapshot_staging_for_step(
        &mut self,
    ) -> Result<(), RaftRuntimeError> {
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
    ///
    /// The catch-all arm passes every other output through untouched and in
    /// place, which is the contract rather than an oversight: kernel output
    /// order is load-bearing, so this rewrites one variant and must not reorder,
    /// drop, or reinterpret anything beside it.
    pub(super) fn resolve_snapshot_chunk_sends(&self, outputs: Vec<RaftOutput>) -> Vec<RaftOutput> {
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
}
