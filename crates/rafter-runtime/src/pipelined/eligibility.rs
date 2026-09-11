//! Only ordinary multi-voter leader appends may precede their local data fence.
use crate::{hard_state::hard_state_for_node, DurableRaftNode};
use rafter::{
    ClientProposalInput, LogIndex, MembershipConfig, Message, NodeId, Output, Role,
    SnapshotChunkSource, Term,
};
use rafter_storage::{RaftHardStateStore, RaftLogSegment, RaftSnapshotStore};

pub(super) fn eligible<
    H: RaftHardStateStore,
    L: RaftLogSegment,
    S: RaftSnapshotStore + SnapshotChunkSource,
>(
    node: &DurableRaftNode<H, L, S>,
    proposals: &[ClientProposalInput],
) -> bool {
    let snapshot = node.snapshot_transfer_status();
    node.fatal_error.is_none()
        && node.role() == Role::Leader
        && matches!(node.effective_membership(), MembershipConfig::Stable(ref set) if set.voters().len() > 1)
        && node.effective_configuration_entry() == node.committed_configuration_entry()
        && snapshot.leader.is_empty()
        && snapshot.follower.is_none()
        && node
            .snapshot_store
            .current_pending_snapshot_transfer()
            .is_none()
        && node.hard_state_store.current() == hard_state_for_node(&node.node)
        && node
            .persisted_tail
            .is_some_and(|tail| tail.still_matches(&node.node))
        && !proposals.is_empty()
        && proposals.len() <= 64
        && proposals
            .iter()
            .try_fold(0usize, |sum, p| {
                sum.checked_add(p.payload.len().saturating_add(64))
            })
            .is_some_and(|bytes| bytes <= 256 * 1024)
}

// Exhaustive dependency classifier: new outputs default to no behavior until reviewed.
pub(super) fn speculative(output: &Output, id: NodeId, term: Term, committed: LogIndex) -> bool {
    match output {
        Output::Send { message, .. } => matches!(message, Message::AppendEntries(request)
            if request.leader_id == id && request.term == term
                && request.leader_commit <= committed && !request.entries.is_empty()
                && request.entries.iter().all(|entry| entry.application_payload().is_some())),
        Output::SendSnapshotChunk { .. }
        | Output::StageSnapshotChunk { .. }
        | Output::ApplySnapshot { .. }
        | Output::ConfigurationCommitted { .. }
        | Output::LocalProposalAppended { .. }
        | Output::LocalProposalDropped { .. }
        | Output::Apply { .. }
        | Output::RejectProposal { .. }
        | Output::LeadershipTransferRejected { .. }
        | Output::ReadIndexGranted { .. }
        | Output::ReadIndexRejected { .. }
        | Output::ReadIndexCanceled { .. } => false,
    }
}
