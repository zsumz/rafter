//! Submitting proposals to the runtime, and the outcomes it reports back.
//!
//! Preflight is what makes a batch atomic at this layer: IDs and encoding
//! are checked before the runtime is touched, so a rejected batch consumes
//! no identity. A drop the runtime does report is an unknown outcome, never
//! a refusal — the entry may already be durable.

use super::{
    Arc, ClientProposalInput, Debug, GroupError, GroupResult, GroupStepReport,
    LocalProposalDropReason, LocalProposalId, LogIndex, PersistedRaftRuntime, Proposal,
    ProposalEvent, ProposalUnknownOutcomeReason, RaftGroup, RaftInput, RaftOutput,
    ReplicatedStateMachine, StateMachineOperation, Term,
};

impl<G, A, R> RaftGroup<G, A, R>
where
    G: Clone + Ord + Debug + Send + Sync + 'static,
    A: ReplicatedStateMachine,
    A::CommandResult: Clone,
    R: PersistedRaftRuntime,
{
    pub(super) fn step_proposal(
        &mut self,
        proposal: &Proposal<A::Command>,
    ) -> GroupResult<A, R, Vec<RaftOutput>> {
        if let Some(last_seen_local_proposal_id) = self.last_seen_local_proposal_id {
            if proposal.local_proposal_id <= last_seen_local_proposal_id {
                return Err(GroupError::NonMonotonicLocalProposalId {
                    local_proposal_id: proposal.local_proposal_id,
                    last_seen_local_proposal_id,
                });
            }
        }
        let payload = self
            .app
            .encode_command(&proposal.command)
            .map_err(|source| GroupError::StateMachine {
                operation: StateMachineOperation::EncodeCommand,
                source: Arc::new(source),
            })?;
        self.last_seen_local_proposal_id = Some(proposal.local_proposal_id);
        self.pending_proposals
            .insert(proposal.local_proposal_id, proposal.client_request_id);
        match self.raft.step(RaftInput::TrackedClientProposal {
            proposal_id: proposal.local_proposal_id,
            payload,
        }) {
            Ok(outputs) => Ok(outputs),
            Err(error) => {
                self.pending_proposals.remove(&proposal.local_proposal_id);
                Err(GroupError::Runtime(error))
            }
        }
    }

    pub(super) fn step_proposals(
        &mut self,
        proposals: Vec<Proposal<A::Command>>,
    ) -> GroupResult<A, R, Vec<RaftOutput>> {
        if proposals.is_empty() {
            return Ok(Vec::new());
        }

        let mut last_seen = self.last_seen_local_proposal_id;
        let mut batch = Vec::with_capacity(proposals.len());
        let mut pending = Vec::with_capacity(proposals.len());

        for proposal in proposals {
            if let Some(last_seen_local_proposal_id) = last_seen {
                if proposal.local_proposal_id <= last_seen_local_proposal_id {
                    return Err(GroupError::NonMonotonicLocalProposalId {
                        local_proposal_id: proposal.local_proposal_id,
                        last_seen_local_proposal_id,
                    });
                }
            }
            let payload = self
                .app
                .encode_command(&proposal.command)
                .map_err(|source| GroupError::StateMachine {
                    operation: StateMachineOperation::EncodeCommand,
                    source: Arc::new(source),
                })?;
            last_seen = Some(proposal.local_proposal_id);
            batch.push(ClientProposalInput {
                proposal_id: Some(proposal.local_proposal_id),
                payload,
            });
            pending.push((proposal.local_proposal_id, proposal.client_request_id));
        }

        self.last_seen_local_proposal_id = last_seen;
        for (local_proposal_id, client_request_id) in pending.iter().copied() {
            self.pending_proposals
                .insert(local_proposal_id, client_request_id);
        }

        match self.raft.step_proposal_batch(batch) {
            Ok(outputs) => Ok(outputs),
            Err(error) => {
                for (local_proposal_id, _) in pending {
                    self.pending_proposals.remove(&local_proposal_id);
                }
                Err(GroupError::Runtime(error))
            }
        }
    }

    pub(super) fn record_unknown_proposal_outcome(
        &mut self,
        proposal_id: LocalProposalId,
        index: LogIndex,
        term: Term,
        drop_reason: LocalProposalDropReason,
        report: &mut GroupStepReport<G, A::CommandResult>,
    ) {
        let Some(client_request_id) = self.pending_proposals.remove(&proposal_id) else {
            return;
        };
        report.proposal_events.push(ProposalEvent::UnknownOutcome {
            local_proposal_id: proposal_id,
            client_request_id,
            reason: ProposalUnknownOutcomeReason::LocalProposalDropped {
                index,
                term,
                reason: drop_reason,
            },
        });
    }
}
