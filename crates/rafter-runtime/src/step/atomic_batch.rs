//! Conservative shared-WAL fast path; exceptional transitions retain explicit fences.
use crate::{log_repair::PersistedTail, DurableRaftNode, RaftRuntimeError};
use rafter::{LogEntryKind, LogIndex, Output, SnapshotChunkSource};
use rafter_storage::{
    durable_batch::{RaftPersistenceBatch, RaftPersistenceBatchError},
    BorrowedPersistedRaftLogEntry, RaftHardState, RaftHardStateStore, RaftLogSegment,
    RaftSnapshotStore,
};

impl<H: RaftHardStateStore, L: RaftLogSegment, S: RaftSnapshotStore + SnapshotChunkSource>
    DurableRaftNode<H, L, S>
{
    pub(super) fn try_atomic_append_batch(
        &mut self,
        before: RaftHardState,
        current: RaftHardState,
        outputs: &[Output],
    ) -> Result<bool, RaftRuntimeError> {
        let Some(domain) = self.hard_state_store.persistence_domain() else {
            return Ok(false);
        };
        if before.current_term != current.current_term
            || before.voted_for != current.voted_for
            || before.committed_configuration != current.committed_configuration
            || outputs.iter().any(needs_explicit_fence)
            || self.node.pending_snapshot_transfer().is_some()
            || self
                .snapshot_store
                .current_pending_snapshot_transfer()
                .is_some()
            || self.node.snapshot_index() != self.log_segment.compacted_through()
            || !self
                .persisted_tail
                .is_some_and(|tail| tail.still_matches(&self.node))
        {
            return Ok(false);
        }
        let first = self.log_segment.next_index();
        let entries = self.node.log_entries_slice_from(first);
        if entries
            .iter()
            .any(|entry| !matches!(entry.kind, LogEntryKind::Application(_)))
        {
            return Ok(false);
        }
        let borrowed: Vec<_> = entries
            .iter()
            .enumerate()
            .map(|(offset, entry)| {
                BorrowedPersistedRaftLogEntry::new(
                    LogIndex(first.0 + offset as u64),
                    entry.term,
                    &entry.kind,
                )
            })
            .collect();
        let batch = RaftPersistenceBatch {
            truncate_from: None,
            entries: &borrowed,
            hard_state: (current != before).then_some(current),
        };
        let Some(receipt) = self
            .log_segment
            .persist_batch(&domain, batch)
            .map_err(RaftRuntimeError::PersistenceBatch)?
        else {
            return Ok(false);
        };
        if receipt.domain != domain
            || receipt.hard_state != current
            || self.hard_state_store.current() != current
            || receipt.next_index != self.node.last_log_index().next()
            || receipt.next_index != self.log_segment.next_index()
            || receipt.compacted_through != self.node.snapshot_index()
            || receipt.compacted_through != self.log_segment.compacted_through()
        {
            return Err(RaftRuntimeError::PersistenceBatch(
                RaftPersistenceBatchError::InvalidBatch("receipt does not match stepped state"),
            ));
        }
        self.persisted_tail = Some(PersistedTail::of_node(&self.node));
        Ok(true)
    }
}

// Deliberately exhaustive: a new output must choose whether this path can fence it.
fn needs_explicit_fence(output: &Output) -> bool {
    match output {
        Output::StageSnapshotChunk { .. }
        | Output::ApplySnapshot { .. }
        | Output::ConfigurationCommitted { .. } => true,
        Output::SendSnapshotChunk { .. }
        | Output::LocalProposalAppended { .. }
        | Output::LocalProposalDropped { .. }
        | Output::Apply { .. }
        | Output::RejectProposal { .. }
        | Output::LeadershipTransferRejected { .. }
        | Output::ReadIndexGranted { .. }
        | Output::ReadIndexRejected { .. }
        | Output::ReadIndexCanceled { .. }
        | Output::Send { .. } => false,
    }
}
