use super::{
    report_has_proposal_lifecycle, Arc, ClientProposalInput, Debug, GroupError, GroupResult,
    GroupStepReport, LocalProposalDropReason, LocalProposalId, LogIndex, PersistedRaftRuntime,
    Proposal, ProposalBatchBeginReport, ProposalBatchBeginReportResult, ProposalBegin,
    ProposalBeginReport, ProposalBeginReportResult, ProposalBeginResult, ProposalEvent,
    ProposalUnknownOutcomeReason, RaftGroup, RaftInput, RaftOutput, ReplicatedStateMachine,
    StateMachineOperation, StepReportOptions, Term,
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
    /// applies, snapshot events, leadership-transfer events, or metrics emitted
    /// while starting the proposal.
    ///
    /// **Membership is the one stream it does not discard.** The report it drops
    /// is a report no caller received, so the membership delta it carried is put
    /// back and stays owed — the next report, or
    /// [`RaftGroup::drain_membership_events`], still carries it. Every other
    /// stream here is genuinely lost, which is why this helper is for callers
    /// that route no peer traffic and hold no other waiters.
    ///
    /// # Errors
    ///
    /// Returns a group error when the group is poisoned, the state machine is
    /// below the runtime's snapshot boundary, command encoding fails, the
    /// runtime rejects the proposal input, or applying a synchronously committed
    /// entry fails. A runtime that accepts the input but emits no lifecycle event
    /// is returned as [`ProposalUnknownOutcomeReason::ProposalDidNotStart`].
    #[allow(clippy::needless_pass_by_value)]
    pub fn begin_proposal_outcome(
        &mut self,
        proposal: Proposal<A::Command>,
    ) -> ProposalBeginResult<G, A, R> {
        // Taken before the step, because everything below discards a full report
        // and a report a caller never receives reported nothing.
        let mark = self.membership_report_mark();
        let ProposalBeginReport { begin, report } = self.begin_proposal(proposal)?;
        self.restore_membership_report_mark(mark, &report);
        Ok(begin)
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
    /// Returns a group error when the group is poisoned, the state machine is
    /// below the runtime's snapshot boundary, command encoding fails, the
    /// runtime rejects the proposal input, or applying a synchronously committed
    /// entry fails. A runtime that accepts the input but emits no lifecycle event
    /// is returned as [`ProposalUnknownOutcomeReason::ProposalDidNotStart`].
    #[allow(clippy::needless_pass_by_value)]
    pub fn begin_proposal(
        &mut self,
        proposal: Proposal<A::Command>,
    ) -> ProposalBeginReportResult<G, A, R> {
        self.reject_if_poisoned()?;
        // This steps the runtime without routing through `step_with_options`,
        // so it takes the boundary verdict by name rather than inheriting it.
        self.reject_if_below_snapshot_boundary()?;
        let local_proposal_id = proposal.local_proposal_id;
        let outputs = self.step_proposal(&proposal)?;
        let mut report =
            self.apply_stepped_outputs(outputs, false, StepReportOptions::default())?;
        self.record_missing_proposal_lifecycles(&[local_proposal_id], &mut report);
        let begin = self.proposal_begin_from_report(local_proposal_id, &report);
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
    /// Returns a group error when the group is poisoned, the state machine is
    /// below the runtime's snapshot boundary, the batch violates local proposal
    /// ID monotonicity, command encoding fails, the runtime rejects the batched
    /// step, or applying synchronously committed entries fails. A proposal for
    /// which the runtime emits no lifecycle event is returned as an unknown
    /// outcome without discarding the other proposals' report content.
    pub fn begin_proposal_batch(
        &mut self,
        proposals: Vec<Proposal<A::Command>>,
    ) -> ProposalBatchBeginReportResult<G, A, R> {
        self.reject_if_poisoned()?;
        // As [`RaftGroup::begin_proposal`]: a stepping entry point that does
        // not route through `step_with_options`.
        self.reject_if_below_snapshot_boundary()?;
        let local_proposal_ids = proposals
            .iter()
            .map(|proposal| proposal.local_proposal_id)
            .collect::<Vec<_>>();
        let outputs = self.step_proposals(proposals)?;
        let mut report =
            self.apply_stepped_outputs(outputs, false, StepReportOptions::default())?;
        self.record_missing_proposal_lifecycles(&local_proposal_ids, &mut report);
        let begins = local_proposal_ids
            .into_iter()
            .map(|local_proposal_id| self.proposal_begin_from_report(local_proposal_id, &report))
            .collect();
        Ok(ProposalBatchBeginReport { begins, report })
    }

    pub(super) fn proposal_begin_from_report(
        &self,
        local_proposal_id: LocalProposalId,
        report: &GroupStepReport<G, A::CommandResult>,
    ) -> ProposalBegin<G, A::CommandResult> {
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
            return ProposalBegin::Completed {
                group_id: self.group_id.clone(),
                local_proposal_id,
                index,
                term,
                result,
                peer_messages: report.peer_messages.clone(),
            };
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
            return ProposalBegin::UnknownOutcome {
                group_id: self.group_id.clone(),
                local_proposal_id,
                client_request_id,
                reason,
                peer_messages: report.peer_messages.clone(),
            };
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
            return ProposalBegin::Appended {
                group_id: self.group_id.clone(),
                local_proposal_id,
                index,
                term,
                peer_messages: report.peer_messages.clone(),
            };
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
            return ProposalBegin::Rejected {
                group_id: self.group_id.clone(),
                local_proposal_id,
                reason,
                leader_hint,
            };
        }

        unreachable!("proposal lifecycle normalization must cover every submitted proposal")
    }

    /// Turns runtime silence into an explicit unknown outcome.
    ///
    /// The runtime has accepted each ID, so no lifecycle output is not proof
    /// that the proposal failed. Recording the uncertainty inside the report
    /// preserves every co-emitted event and gives every caller one ordinary
    /// lifecycle stream to route.
    pub(super) fn record_missing_proposal_lifecycles(
        &mut self,
        local_proposal_ids: &[LocalProposalId],
        report: &mut GroupStepReport<G, A::CommandResult>,
    ) {
        for local_proposal_id in local_proposal_ids {
            if report_has_proposal_lifecycle(*local_proposal_id, report) {
                continue;
            }
            let client_request_id = self.pending_proposals.remove(local_proposal_id).flatten();
            report.proposal_events.push(ProposalEvent::UnknownOutcome {
                local_proposal_id: *local_proposal_id,
                client_request_id,
                reason: ProposalUnknownOutcomeReason::ProposalDidNotStart,
            });
        }
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
