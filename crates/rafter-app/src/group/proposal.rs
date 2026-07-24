use super::{
    report_has_proposal_lifecycle, ClientProposalInput, Debug, GroupError, GroupResult,
    GroupStepReport, LocalProposalDropReason, LocalProposalId, LogIndex, PersistedRaftRuntime,
    Proposal, ProposalBatchBeginReport, ProposalBatchBeginReportResult, ProposalBegin,
    ProposalBeginReport, ProposalBeginReportResult, ProposalBeginResult, ProposalEvent,
    ProposalUnknownOutcomeReason, RaftGroup, RaftInput, RaftOutput, ReplicatedStateMachine,
    StateMachineOperation, Term,
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

    /// Begins a batch of local tracked proposals in one runtime batch.
    ///
    /// The batch is preflighted before touching the runtime: proposal IDs
    /// must be strictly increasing above this group's watermark and every
    /// command must encode successfully. Once preflight succeeds, every
    /// proposal ID in the batch is consumed and submitted to the persisted
    /// runtime under one [`PersistedRaftRuntime::step_proposal_batch`] call.
    ///
    /// # Errors
    ///
    /// Returns a group error when the group is poisoned, the batch violates
    /// local proposal ID monotonicity, command encoding fails, the runtime
    /// rejects the batched step, applying synchronously committed entries
    /// fails, or the runtime produces no proposal lifecycle event for any
    /// supplied local proposal ID.
    pub fn begin_proposal_batch(
        &mut self,
        proposals: Vec<Proposal<A::Command>>,
    ) -> ProposalBatchBeginReportResult<G, A, R> {
        self.reject_if_poisoned()?;
        let previous_effective = self.raft.membership();
        let previous_committed = self.raft.committed_membership();
        let local_proposal_ids = proposals
            .iter()
            .map(|proposal| proposal.local_proposal_id)
            .collect::<Vec<_>>();
        let outputs = self.step_proposals(proposals)?;
        let report = self.apply_raft_outputs_after_step(
            outputs,
            previous_effective,
            previous_committed,
            false,
        )?;
        self.ensure_proposal_batch_lifecycles(&local_proposal_ids, &report)?;
        let mut begins = Vec::with_capacity(local_proposal_ids.len());
        for local_proposal_id in local_proposal_ids {
            begins.push(self.proposal_begin_from_report(local_proposal_id, &report)?);
        }
        Ok(ProposalBatchBeginReport { begins, report })
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

        // The hint comes from the event rather than a post-step re-read, so the
        // immediate and asynchronous views of one rejection cannot disagree.
        if let Some((reason, leader_hint)) =
            report.proposal_events.iter().find_map(|event| match event {
                ProposalEvent::Rejected {
                    local_proposal_id: id,
                    reason,
                    leader_hint,
                } if *id == local_proposal_id => Some((reason.clone(), *leader_hint)),
                _ => None,
            })
        {
            return Ok(ProposalBegin::Rejected {
                group_id: self.group_id.clone(),
                local_proposal_id,
                reason,
                leader_hint,
            });
        }

        if !report_has_proposal_lifecycle(local_proposal_id, report) {
            self.pending_proposals.remove(&local_proposal_id);
        }
        Err(GroupError::ProposalDidNotStart { local_proposal_id })
    }

    pub(super) fn ensure_proposal_batch_lifecycles(
        &mut self,
        local_proposal_ids: &[LocalProposalId],
        report: &GroupStepReport<G, A::CommandResult>,
    ) -> GroupResult<A, R, ()> {
        let Some(local_proposal_id) = local_proposal_ids
            .iter()
            .copied()
            .find(|id| !report_has_proposal_lifecycle(*id, report))
        else {
            return Ok(());
        };

        for pending_id in local_proposal_ids {
            self.pending_proposals.remove(pending_id);
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
                    source,
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
