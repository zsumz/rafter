use super::{
    report_has_proposal_lifecycle, ApplyEntry, Debug, GroupError, GroupInput, GroupResult,
    GroupStepReport, LeadershipTransferEvent, LogIndex, MembershipConfig, MembershipStepContext,
    Message, NodeId, PeerEnvelope, PersistedRaftRuntime, Proposal, ProposalEvent, RaftGroup,
    RaftGroupMetrics, RaftInput, RaftOutput, ReplicatedStateMachine, SnapshotEvent,
    StepReportOptions, StepReportResult,
};

impl<G, A, R> RaftGroup<G, A, R>
where
    G: Clone + Ord + Debug + Send + Sync + 'static,
    A: ReplicatedStateMachine,
    A::CommandResult: Clone,
    R: PersistedRaftRuntime,
{
    /// Returns a point-in-time metrics snapshot for the group.
    #[must_use]
    pub fn metrics(&self) -> RaftGroupMetrics<G> {
        let applied_index = self.app.applied_index().unwrap_or(self.last_applied_index);
        let pending_read_barriers = self.pending_reads.len();
        RaftGroupMetrics {
            group_id: self.group_id.clone(),
            node_id: self.node_id,
            role: self.raft.role(),
            term: self.raft.current_term(),
            leader_hint: self.raft.leader_hint(),
            commit_index: self.raft.commit_index(),
            applied_index,
            last_log_index: self.raft.last_log_index(),
            snapshot_index: self.raft.snapshot_index(),
            membership: self.raft.membership(),
            replication: self.raft.replication(),
            pending_proposals: self.pending_proposals.len(),
            pending_reads: pending_read_barriers,
            pending_read_barriers,
            pending_query_reads: self.pending_query_reads.len(),
            completed_query_reads: self.completed_query_reads.len(),
            reserved_reads: self.reserved_read_count(),
            fatal_state: self.fatal_state.clone(),
        }
    }

    /// Returns the index this group's state machine must reach to have applied
    /// every committed application command.
    ///
    /// Compare it with the state machine's own applied index to gate readiness
    /// after recovery:
    ///
    /// ```text
    /// state_machine.applied_index()? >= group.committed_application_index()
    /// ```
    ///
    /// Use `>=`, never equality. A state machine that installed a snapshot whose
    /// boundary sits above the last committed application entry legitimately
    /// reports a higher applied index, as does one seeded through
    /// [`RaftGroup::with_applied_index`].
    ///
    /// The predicate is false while a restarted node still holds recovery outputs
    /// the caller has not applied, which is exactly when a readiness gate must hold
    /// a replica back. It is not a linearizability signal: it proves only that this
    /// replica has applied everything *it* knows to be committed. Group poison does
    /// not change it — a poisoned group reports the same runtime value and will
    /// never apply again, so a readiness gate must check
    /// [`RaftGroup::fatal_state`] as well.
    ///
    /// A caller that compacts at a boundary above the index its state machine
    /// reports applied raises this value past what that state machine will ever
    /// reach. Compact at the applied index, as
    /// [`crate::state_machine::ReplicatedStateMachine::build_snapshot`] already
    /// requires.
    #[must_use]
    pub fn committed_application_index(&self) -> LogIndex {
        self.raft.committed_application_index()
    }

    /// Steps one group input and returns all explicit side effects.
    ///
    /// # Errors
    ///
    /// Returns a group error when the group is poisoned, an input targets the
    /// wrong group or node, the runtime rejects the underlying Raft input, or
    /// the state machine fails while encoding, applying, reading, or installing
    /// snapshot data.
    pub fn step(&mut self, input: GroupInput<G, A::Command>) -> StepReportResult<G, A, R> {
        self.step_with_options(input, StepReportOptions::default())
    }

    /// Steps one group input with explicit report materialization options.
    ///
    /// This preserves the same protocol and application semantics as
    /// [`RaftGroup::step`]. `options` only controls whether observability-only
    /// fields such as the metrics snapshot are materialized in the returned
    /// report.
    ///
    /// # Errors
    ///
    /// Returns a group error when the group is poisoned, an input targets the
    /// wrong group or node, the runtime rejects the underlying Raft input, or
    /// the state machine fails while encoding, applying, reading, or installing
    /// snapshot data.
    pub fn step_with_options(
        &mut self,
        input: GroupInput<G, A::Command>,
        options: StepReportOptions,
    ) -> StepReportResult<G, A, R> {
        self.reject_if_poisoned()?;
        let previous_effective = self.raft.membership();
        let previous_committed = self.raft.committed_membership();
        match input {
            GroupInput::Tick => {
                let outputs = self
                    .raft
                    .step(RaftInput::Tick)
                    .map_err(GroupError::Runtime)?;
                self.apply_raft_outputs_after_step_with_options(
                    outputs,
                    previous_effective,
                    previous_committed,
                    false,
                    options,
                )
            }
            GroupInput::PeerMessage { envelope } => {
                self.validate_peer_envelope(&envelope)?;
                let outputs = self
                    .raft
                    .step(RaftInput::Message {
                        from: envelope.from,
                        message: envelope.message,
                    })
                    .map_err(GroupError::Runtime)?;
                self.apply_raft_outputs_after_step_with_options(
                    outputs,
                    previous_effective,
                    previous_committed,
                    false,
                    options,
                )
            }
            GroupInput::Proposal { proposal } => {
                self.step_proposal_input(&proposal, previous_effective, previous_committed, options)
            }
            GroupInput::ProposalBatch { proposals } => self.step_proposal_batch_input(
                proposals,
                previous_effective,
                previous_committed,
                options,
            ),
            GroupInput::ReadBarrier { request } => self.step_read_barrier_input(
                &request,
                previous_effective,
                previous_committed,
                options,
            ),
            GroupInput::TransferLeadership { target } => {
                let outputs = self
                    .raft
                    .step(RaftInput::TransferLeadership { target })
                    .map_err(GroupError::Runtime)?;
                let mut report = self.apply_raft_outputs_after_step_with_options(
                    outputs,
                    previous_effective,
                    previous_committed,
                    false,
                    options,
                )?;
                if !report
                    .leadership_transfer_events
                    .iter()
                    .any(|event| matches!(event, LeadershipTransferEvent::Rejected { target: rejected_target, .. } if *rejected_target == target))
                {
                    report
                        .leadership_transfer_events
                        .push(LeadershipTransferEvent::Started { target });
                }
                Ok(report)
            }
            GroupInput::Membership { change } => {
                let outputs = self
                    .raft
                    .step(Self::membership_change_input(change))
                    .map_err(GroupError::Runtime)?;
                self.apply_raft_outputs_after_step_with_options(
                    outputs,
                    previous_effective,
                    previous_committed,
                    true,
                    options,
                )
            }
        }
    }

    fn step_proposal_input(
        &mut self,
        proposal: &Proposal<A::Command>,
        previous_effective: MembershipConfig,
        previous_committed: MembershipConfig,
        options: StepReportOptions,
    ) -> StepReportResult<G, A, R> {
        let local_proposal_id = proposal.local_proposal_id;
        let outputs = self.step_proposal(proposal)?;
        let report = self.apply_raft_outputs_after_step_with_options(
            outputs,
            previous_effective,
            previous_committed,
            false,
            options,
        )?;
        if !report_has_proposal_lifecycle(local_proposal_id, &report) {
            self.pending_proposals.remove(&local_proposal_id);
            return Err(GroupError::ProposalDidNotStart { local_proposal_id });
        }
        Ok(report)
    }

    fn step_proposal_batch_input(
        &mut self,
        proposals: Vec<Proposal<A::Command>>,
        previous_effective: MembershipConfig,
        previous_committed: MembershipConfig,
        options: StepReportOptions,
    ) -> StepReportResult<G, A, R> {
        let local_proposal_ids = proposals
            .iter()
            .map(|proposal| proposal.local_proposal_id)
            .collect::<Vec<_>>();
        let outputs = self.step_proposals(proposals)?;
        let report = self.apply_raft_outputs_after_step_with_options(
            outputs,
            previous_effective,
            previous_committed,
            false,
            options,
        )?;
        self.ensure_proposal_batch_lifecycles(&local_proposal_ids, &report)?;
        Ok(report)
    }

    /// Applies outputs already produced by the durable Raft runtime.
    ///
    /// This is an advanced direct path for callers that drive
    /// [`PersistedRaftRuntime`] themselves. Normal callers should prefer
    /// [`RaftGroup::step`], [`RaftGroup::begin_proposal`],
    /// [`RaftGroup::begin_read_barrier`], or [`RaftGroup::read`], which
    /// generate and apply runtime outputs in one poison-checked operation.
    ///
    /// The `outputs` vector must preserve the exact order returned by the
    /// runtime step that produced it. Kernel output ordering is load-bearing:
    /// for example, snapshot chunk staging and snapshot apply events can be
    /// paired with messages emitted by the same step, and callers must not
    /// reorder, drop, or replay raw outputs unless they also own the resulting
    /// protocol and application semantics.
    ///
    /// # Errors
    ///
    /// Returns a group error when the group is poisoned, output handling
    /// detects malformed snapshot data, command decode fails, the state
    /// machine apply/install path fails, or completed reads cannot be served.
    pub fn apply_raft_outputs(&mut self, outputs: Vec<RaftOutput>) -> StepReportResult<G, A, R> {
        self.reject_if_poisoned()?;
        self.apply_raft_outputs_with_membership_context(outputs, None)
    }

    pub(super) fn apply_raft_outputs_after_step(
        &mut self,
        outputs: Vec<RaftOutput>,
        previous_effective: MembershipConfig,
        previous_committed: MembershipConfig,
        membership_request: bool,
    ) -> StepReportResult<G, A, R> {
        self.apply_raft_outputs_after_step_with_options(
            outputs,
            previous_effective,
            previous_committed,
            membership_request,
            StepReportOptions::default(),
        )
    }

    pub(super) fn apply_raft_outputs_after_step_with_options(
        &mut self,
        outputs: Vec<RaftOutput>,
        previous_effective: MembershipConfig,
        previous_committed: MembershipConfig,
        membership_request: bool,
        options: StepReportOptions,
    ) -> StepReportResult<G, A, R> {
        self.apply_raft_outputs_with_membership_context_and_options(
            outputs,
            Some(MembershipStepContext {
                previous_effective,
                previous_committed,
                membership_request,
            }),
            options,
        )
    }

    pub(super) fn apply_raft_outputs_with_membership_context(
        &mut self,
        outputs: Vec<RaftOutput>,
        membership_context: Option<MembershipStepContext>,
    ) -> StepReportResult<G, A, R> {
        self.apply_raft_outputs_with_membership_context_and_options(
            outputs,
            membership_context,
            StepReportOptions::default(),
        )
    }

    pub(super) fn apply_raft_outputs_with_membership_context_and_options(
        &mut self,
        outputs: Vec<RaftOutput>,
        membership_context: Option<MembershipStepContext>,
        options: StepReportOptions,
    ) -> StepReportResult<G, A, R> {
        let membership_request = membership_context
            .as_ref()
            .is_some_and(|context| context.membership_request);
        let mut report = GroupStepReport::new(self.group_id.clone());
        let mut apply_entries = Vec::new();

        for output in outputs {
            self.record_raft_output(output, membership_request, &mut apply_entries, &mut report)?;
        }

        self.apply_entries(apply_entries, &mut report)?;
        self.complete_ready_reads(&mut report)?;
        if let Some(context) = membership_context {
            self.record_membership_changes(&context, &mut report);
        }
        if options.include_metrics {
            report.metrics = Some(self.metrics());
        }
        Ok(report)
    }

    pub(super) fn record_raft_output(
        &mut self,
        output: RaftOutput,
        membership_request: bool,
        apply_entries: &mut Vec<ApplyEntry<A::Command>>,
        report: &mut GroupStepReport<G, A::CommandResult>,
    ) -> GroupResult<A, R, ()> {
        match output {
            RaftOutput::Send { to, message } => {
                self.record_peer_message(to, message, report);
            }
            RaftOutput::LocalProposalAppended {
                proposal_id,
                index,
                term,
            } => {
                if self.pending_proposals.contains_key(&proposal_id) {
                    report.proposal_events.push(ProposalEvent::Appended {
                        local_proposal_id: proposal_id,
                        index,
                        term,
                    });
                }
            }
            RaftOutput::LocalProposalDropped {
                proposal_id,
                index,
                term,
                reason,
            } => {
                self.record_unknown_proposal_outcome(proposal_id, index, term, reason, report);
            }
            RaftOutput::RejectProposal {
                proposal_id: Some(proposal_id),
                reason,
            } => {
                if self.pending_proposals.remove(&proposal_id).is_some() {
                    report.proposal_events.push(ProposalEvent::Rejected {
                        local_proposal_id: proposal_id,
                        reason,
                        leader_hint: self.raft.leader_hint(),
                    });
                }
            }
            RaftOutput::RejectProposal {
                proposal_id: None,
                reason,
            } if membership_request => {
                self.record_membership_rejection(reason, report);
            }
            RaftOutput::RejectProposal {
                proposal_id: None, ..
            } => {}
            RaftOutput::LeadershipTransferRejected { target, reason } => {
                report
                    .leadership_transfer_events
                    .push(LeadershipTransferEvent::Rejected {
                        target,
                        reason,
                        leader_hint: self.raft.leader_hint(),
                    });
            }
            RaftOutput::Apply {
                index,
                term,
                payload,
                local_proposal_id,
            } => {
                apply_entries.push(self.decode_apply_output(
                    index,
                    term,
                    &payload,
                    local_proposal_id,
                )?);
            }
            RaftOutput::ReadIndexGranted {
                read_id,
                read_index,
            } => {
                if let Some(pending) = self.pending_reads.get_mut(&read_id) {
                    pending.read_index = Some(read_index);
                }
            }
            RaftOutput::ReadIndexRejected { read_id, reason } => {
                self.record_rejected_read(read_id, reason, report);
            }
            RaftOutput::ReadIndexCanceled { read_id, reason } => {
                self.record_canceled_read(read_id, reason, report);
            }
            RaftOutput::ApplySnapshot { snapshot } => {
                self.apply_snapshot_output(snapshot, report)?;
            }
            RaftOutput::StageSnapshotChunk { chunk } => {
                report.snapshot_events.push(SnapshotEvent::StageChunk {
                    group_id: self.group_id.clone(),
                    chunk,
                });
            }
            RaftOutput::SendSnapshotChunk { to, chunk } => {
                report.snapshot_events.push(SnapshotEvent::SendChunk {
                    group_id: self.group_id.clone(),
                    to,
                    chunk,
                });
            }
        }
        Ok(())
    }
    pub(super) fn record_peer_message(
        &self,
        to: NodeId,
        message: Message,
        report: &mut GroupStepReport<G, A::CommandResult>,
    ) {
        report.peer_messages.push(PeerEnvelope {
            group_id: self.group_id.clone(),
            from: self.node_id,
            to,
            message,
        });
    }
}
