//! Ownership blocks quorum processing until the matching local durability completion.
use super::{
    eligibility, PersistenceCompletion, PersistenceOperation, PersistenceWork, PipelineError,
    PreparedProposals,
};
use crate::{hard_state::hard_state_for_node, DurableRaftNode};
use rafter::{ClientProposalInput, Input, LogIndex, Output, Role, SnapshotChunkSource};
use rafter_storage::{
    durable_batch::PersistenceDomain, RaftHardStateStore, RaftLogSegment, RaftSnapshotStore,
};

/// Distinct local positions; application durable progress remains application-owned.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PipelineProgress {
    /// Last entry accepted by the kernel, possibly still awaiting persistence.
    pub accepted: LogIndex,
    /// Last entry handed to persistence (synchronous or asynchronous).
    pub submitted: LogIndex,
    /// Last entry acknowledged durable by local storage.
    pub durable: LogIndex,
    /// Commit position acknowledged durable by local storage.
    pub committed: LogIndex,
}

/// Bounded owner for one prepared write and its explicit completion dependency.
///
/// Only leader proposal replication can escape before local persistence. While
/// work is outstanding, no consensus input can run: in particular, follower
/// acknowledgments cannot count the unstable local copy toward quorum.
#[derive(Debug)]
pub struct PipelinedRaftNode<H, L, S> {
    node: Option<DurableRaftNode<H, L, S>>,
    generation: PersistenceDomain,
    sequence: u64,
    pending: Option<PersistenceOperation>,
    progress: PipelineProgress,
}
impl<H: RaftHardStateStore, L: RaftLogSegment, S: RaftSnapshotStore + SnapshotChunkSource>
    PipelinedRaftNode<H, L, S>
{
    /// Wraps an already recovered runtime in a new completion generation.
    #[must_use]
    pub fn new(node: DurableRaftNode<H, L, S>) -> Self {
        let progress = positions(&node);
        Self {
            node: Some(node),
            generation: PersistenceDomain::new(),
            sequence: 0,
            pending: None,
            progress,
        }
    }
    /// Returns the node only when no outstanding write owns its unstable state.
    #[must_use]
    pub fn ready_node(&self) -> Option<&DurableRaftNode<H, L, S>> {
        self.node.as_ref()
    }
    /// Exact pending generation/operation, if persistence currently owns the node.
    #[must_use]
    pub fn pending_operation(&self) -> Option<&PersistenceOperation> {
        self.pending.as_ref()
    }
    /// Returns accepted, submitted, locally durable, and durably committed positions.
    #[must_use]
    pub const fn progress(&self) -> PipelineProgress {
        self.progress
    }
    /// Drives ordinary inputs through the unchanged synchronous fence.
    ///
    /// # Errors
    /// Refuses while persistence is pending, or returns the runtime's fatal error.
    pub fn step_batch(&mut self, inputs: Vec<Input>) -> Result<Vec<Output>, PipelineError> {
        let node = self
            .node
            .as_mut()
            .ok_or(PipelineError::PersistencePending)?;
        let result = node.step_batch(inputs);
        self.progress = positions(node);
        result.map_err(Into::into)
    }
    /// Prepares a bounded proposal batch, exposing only dependency-safe replication.
    ///
    /// # Errors
    /// Refuses while busy or on sequence exhaustion; synchronous failures poison the runtime.
    pub fn prepare_proposals(
        &mut self,
        proposals: Vec<ClientProposalInput>,
    ) -> Result<PreparedProposals<H, L, S>, PipelineError> {
        let node = self
            .node
            .as_mut()
            .ok_or(PipelineError::PersistencePending)?;
        if !eligibility::eligible(node, &proposals) {
            let result = node.step_proposal_batch(proposals);
            self.progress = positions(node);
            return result.map(PreparedProposals::Durable).map_err(Into::into);
        }
        let sequence = self
            .sequence
            .checked_add(1)
            .ok_or(PipelineError::OperationExhausted)?;
        let before = node.hard_state_store.current();
        let commit_floor = node.commit_index();
        let outputs =
            rafter_storage::telemetry::measure(rafter_storage::telemetry::Stage::Kernel, || {
                node.node.step_proposal_batch(proposals)
            });
        let unchanged = hard_state_for_node(&node.node) == before && node.role() == Role::Leader;
        let mut replication = Vec::new();
        let mut deferred = Vec::new();
        for output in outputs {
            if unchanged
                && eligibility::speculative(&output, node.id(), before.current_term, commit_floor)
            {
                replication.push(output);
            } else {
                deferred.push(output);
            }
        }
        if replication.is_empty() {
            let result = node.persist_stepped(before, commit_floor, deferred);
            self.progress = positions(node);
            return result.map(PreparedProposals::Durable).map_err(Into::into);
        }
        self.progress.accepted = node.last_log_index();
        self.progress.submitted = node.last_log_index();
        let node = self.node.take().ok_or(PipelineError::PersistencePending)?;
        let operation = PersistenceOperation {
            generation: self.generation.clone(),
            sequence,
        };
        self.sequence = sequence;
        self.pending = Some(operation.clone());
        Ok(PreparedProposals::Pending {
            replication,
            work: Box::new(PersistenceWork {
                operation,
                node,
                before,
                commit_floor,
                deferred,
            }),
        })
    }
    /// Accepts exactly the outstanding generation/operation, then releases fenced outputs.
    ///
    /// # Errors
    /// Rejects stale/foreign completions without replacing the current node. A failed
    /// storage completion restores the poisoned runtime and releases no outputs.
    pub fn complete(
        &mut self,
        completion: PersistenceCompletion<H, L, S>,
    ) -> Result<Vec<Output>, PipelineError> {
        if self.pending.as_ref() != Some(&completion.operation) || self.node.is_some() {
            return Err(PipelineError::UnexpectedCompletion);
        }
        self.progress = positions(&completion.node);
        self.node = Some(completion.node);
        self.pending = None;
        completion.result.map_err(Into::into)
    }
}
fn positions<
    H: RaftHardStateStore,
    L: RaftLogSegment,
    S: RaftSnapshotStore + SnapshotChunkSource,
>(
    node: &DurableRaftNode<H, L, S>,
) -> PipelineProgress {
    PipelineProgress {
        accepted: node.last_log_index(),
        submitted: node.last_log_index(),
        durable: LogIndex(node.log_segment.next_index().0.saturating_sub(1)),
        committed: node.hard_state_store.current().commit_index,
    }
}
