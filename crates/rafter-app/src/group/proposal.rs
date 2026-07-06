use super::{
    report_has_proposal_lifecycle, Debug, GroupError, GroupResult, GroupStepReport,
    LocalProposalDropReason, LocalProposalId, LogIndex, PersistedRaftRuntime, Proposal,
    ProposalBegin, ProposalBeginReport, ProposalBeginReportResult, ProposalBeginResult,
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
    /// Begins a local tracked proposal and reports its immediate local state.
    ///
    /// This outcome-only helper intentionally discards co-emitted report
    /// streams. Use [`RaftGroup::begin_proposal`] when callers must observe
    /// applies, snapshot events, membership events, leadership-transfer
    /// events, or metrics emitted while starting the proposal.
    ///
    /// # Errors
    ///
    /// Returns a group error when the group is poisoned, command encoding
    /// fails, the runtime rejects the proposal input, applying a synchronously
    /// committed entry fails, or the runtime produces no proposal lifecycle
    /// event for the supplied local proposal ID.
    #[allow(clippy::needless_pass_by_value)]
    pub fn begin_proposal_outcome(
        &mut self,
        proposal: Proposal<A::Command>,
    ) -> ProposalBeginResult<G, A, R> {
        Ok(self.begin_proposal(proposal)?.begin)
    }

    /// Begins a local tracked proposal and returns the immediate state plus
    /// the full step report generated while starting it.
    ///
    /// Use this method when callers must observe co-emitted applies, snapshot
    /// events, membership events, leadership-transfer events, or metrics
    /// instead of only the proposal lifecycle convenience value.
    ///
    /// # Errors
    ///
    /// Returns a group error when the group is poisoned, command encoding
    /// fails, the runtime rejects the proposal input, applying a synchronously
    /// committed entry fails, or the runtime produces no proposal lifecycle
    /// event for the supplied local proposal ID.
    #[allow(clippy::needless_pass_by_value)]
    pub fn begin_proposal(
        &mut self,
        proposal: Proposal<A::Command>,
    ) -> ProposalBeginReportResult<G, A, R> {
        self.reject_if_poisoned()?;
        let previous_effective = self.raft.membership();
        let previous_committed = self.raft.committed_membership();
        let local_proposal_id = proposal.local_proposal_id;
        let outputs = self.step_proposal(&proposal)?;
        let report = self.apply_raft_outputs_after_step(
            outputs,
            previous_effective,
            previous_committed,
            false,
        )?;
        let begin = self.proposal_begin_from_report(local_proposal_id, &report)?;
        Ok(ProposalBeginReport { begin, report })
    }

    pub(super) fn proposal_begin_from_report(
        &mut self,
        local_proposal_id: LocalProposalId,
        report: &GroupStepReport<G, A::CommandResult>,
    ) -> ProposalBeginResult<G, A, R> {
        if let Some(event) = report.proposal_events.iter().find_map(|event| match event {
            ProposalEvent::Applied {
                local_proposal_id: id,
                index,
                term,
                result,
            } if *id == local_proposal_id => Some((*index, *term, result.clone())),
            _ => None,
        }) {
            let (index, term, result) = event;
            return Ok(ProposalBegin::Completed {
                group_id: self.group_id.clone(),
                local_proposal_id,
                index,
                term,
                result,
                peer_messages: report.peer_messages.clone(),
            });
        }

        let unknown_outcome = report.proposal_events.iter().find_map(|event| match event {
            ProposalEvent::UnknownOutcome {
                local_proposal_id: id,
                client_request_id,
                reason,
            } if *id == local_proposal_id => Some((*client_request_id, reason.clone())),
            _ => None,
        });
        if let Some((client_request_id, reason)) = unknown_outcome {
            return Ok(ProposalBegin::UnknownOutcome {
                group_id: self.group_id.clone(),
                local_proposal_id,
                client_request_id,
                reason,
                peer_messages: report.peer_messages.clone(),
            });
        }

        if let Some(event) = report.proposal_events.iter().find_map(|event| match event {
            ProposalEvent::Appended {
                local_proposal_id: id,
                index,
                term,
            } if *id == local_proposal_id => Some((*index, *term)),
            _ => None,
        }) {
            let (index, term) = event;
            return Ok(ProposalBegin::Appended {
                group_id: self.group_id.clone(),
                local_proposal_id,
                index,
                term,
                peer_messages: report.peer_messages.clone(),
            });
        }

        if let Some(reason) = report.proposal_events.iter().find_map(|event| match event {
            ProposalEvent::Rejected {
                local_proposal_id: id,
                reason,
            } if *id == local_proposal_id => Some(reason.clone()),
            _ => None,
        }) {
            return Ok(ProposalBegin::Rejected {
                group_id: self.group_id.clone(),
                local_proposal_id,
                reason,
                leader_hint: self.raft.leader_hint(),
            });
        }

        if !report_has_proposal_lifecycle(local_proposal_id, report) {
            self.pending_proposals.remove(&local_proposal_id);
        }
        Err(GroupError::ProposalDidNotStart { local_proposal_id })
    }
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
                source,
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
