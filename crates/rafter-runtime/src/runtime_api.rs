//! The [`PersistedRaftRuntime`] view of [`DurableRaftNode`].
//!
//! Every method forwards to the inherent one of the same name, so the trait
//! carries no second copy of any rule. It lives beside `lib.rs` rather than in
//! it because the crate root is where a reader lands, and a page of forwarding
//! is not what that reader came for.

use rafter::{
    ClientProposalInput, Input as RaftInput, LogIndex, MembershipConfig, NodeId as RaftNodeId,
    Output as RaftOutput, RaftSnapshot, ReplicationProgress, Role as RaftRole, SnapshotChunkSource,
    Term,
};
use rafter_storage::{RaftHardStateStore, RaftLogSegment, RaftSnapshotStore};

use crate::{DurableRaftNode, PersistedRaftRuntime, RaftRuntimeError};

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

    /// Clones rather than borrows, because the trait's caller holds the runtime
    /// mutably while it repairs the state machine beside it.
    fn snapshot(&self) -> Option<RaftSnapshot> {
        DurableRaftNode::snapshot(self).cloned()
    }

    fn committed_application_index_through(&self, index: LogIndex) -> LogIndex {
        DurableRaftNode::committed_application_index_through(self, index)
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
