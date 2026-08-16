use super::{
    ApplyEntry, Debug, GrantedReadIndex, GroupError, GroupInput, GroupResult, GroupStepReport,
    LeadershipTransferEvent, LocalProposalId, LogIndex, MembershipEvent, Message, NodeId,
    PeerEnvelope, PersistedRaftRuntime, Proposal, ProposalEvent, RaftGroup, RaftGroupMetrics,
    RaftInput, RaftOutput, ReadId, ReplicatedStateMachine, SnapshotEvent, StepReportOptions,
    StepReportResult, Term,
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
    ///
    /// The same per-replica predicate is a cluster harness's convergence wait:
    /// every replica the harness can still reach must satisfy it before the
    /// cluster has settled. Replicas the harness has partitioned or removed
    /// cannot advance and are the caller's to skip.
    #[must_use]
    pub fn committed_application_index(&self) -> LogIndex {
        self.raft.committed_application_index()
    }

    /// Returns the index this group's state machine must reach to have applied
    /// every committed application command at or below `index`.
    ///
    /// Use this to convert a raw log index into a reachable applied floor. A
    /// commit index, a read index, or a snapshot boundary may name an entry the
    /// state machine will never be told about, and waiting for the state machine
    /// to report that index waits forever. An index taken from
    /// [`crate::proposal::ProposalEvent::Applied`] never needs the conversion: it
    /// already names an application entry.
    ///
    /// This is what [`RaftGroup::read`] applies to a granted read index on the
    /// caller's behalf. It is not applied to a caller-supplied
    /// `min_applied_index`; see [`crate::read::ReadRequest`].
    #[must_use]
    pub fn committed_application_index_through(&self, index: LogIndex) -> LogIndex {
        self.raft.committed_application_index_through(index)
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
        self.reject_if_below_snapshot_boundary()?;
        match input {
            GroupInput::Tick => {
                let outputs = self
                    .raft
                    .step(RaftInput::Tick)
                    .map_err(GroupError::Runtime)?;
                self.apply_stepped_outputs(outputs, false, options)
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
                self.apply_stepped_outputs(outputs, false, options)
            }
            GroupInput::Proposal { proposal } => self.step_proposal_input(&proposal, options),
            GroupInput::ProposalBatch { proposals } => {
                self.step_proposal_batch_input(proposals, options)
            }
            GroupInput::ReadBarrier { request } => self.step_read_barrier_input(&request, options),
            GroupInput::TransferLeadership { target } => {
                let outputs = self
                    .raft
                    .step(RaftInput::TransferLeadership { target })
                    .map_err(GroupError::Runtime)?;
                let mut report = self.apply_stepped_outputs(outputs, false, options)?;
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
                self.apply_stepped_outputs(outputs, true, options)
            }
        }
    }

    fn step_proposal_input(
        &mut self,
        proposal: &Proposal<A::Command>,
        options: StepReportOptions,
    ) -> StepReportResult<G, A, R> {
        let local_proposal_id = proposal.local_proposal_id;
        let outputs = self.step_proposal(proposal)?;
        let mut report = self.apply_stepped_outputs(outputs, false, options)?;
        self.record_missing_proposal_lifecycles(&[local_proposal_id], &mut report);
        Ok(report)
    }

    fn step_proposal_batch_input(
        &mut self,
        proposals: Vec<Proposal<A::Command>>,
        options: StepReportOptions,
    ) -> StepReportResult<G, A, R> {
        let local_proposal_ids = proposals
            .iter()
            .map(|proposal| proposal.local_proposal_id)
            .collect::<Vec<_>>();
        let outputs = self.step_proposals(proposals)?;
        let mut report = self.apply_stepped_outputs(outputs, false, options)?;
        self.record_missing_proposal_lifecycles(&local_proposal_ids, &mut report);
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
    /// This never returns
    /// [`GroupError::AppliedIndexBelowSnapshotBoundary`]. That verdict is
    /// permanent, and a state machine below the runtime's snapshot boundary is
    /// a legitimate *transient* here: an inbound snapshot is promoted durably
    /// before the application installs it, so a replica that crashed between
    /// those two writes opens short of a boundary its Raft state already
    /// carries. The permanent verdict is taken instead by [`RaftGroup::step`],
    /// [`RaftGroup::begin_proposal`], [`RaftGroup::begin_proposal_batch`], and
    /// [`RaftGroup::read`], which is where a state machine that was never
    /// restored would first answer for the replica.
    ///
    /// **It does return [`GroupError::SnapshotRestoreRequired`], and that is
    /// not the same statement.** This pump used to accept a committed suffix
    /// on top of that transient, which is how a replica ended up with a
    /// prefix, a hole where the compacted entries were, and a suffix — while
    /// reporting itself caught up. So the transient is tolerated and applying
    /// *through* it is refused: the refusal is raised before the application
    /// is touched, names the entry it stopped in front of, and does not
    /// poison, because the repair is still available. Perform it with
    /// [`RaftGroup::apply_recovery_outputs`], which is the operation a restart
    /// path should be draining its recovery outputs through in the first
    /// place. A batch carrying no committed application entry — the shape a
    /// fully compacted replica hands over — is unaffected.
    ///
    /// # Errors
    ///
    /// Returns a group error when the group is poisoned, a committed
    /// application entry would land on a state machine still short of the
    /// snapshot boundary, output handling detects malformed snapshot data,
    /// command decode fails, the state machine apply/install path fails, or
    /// completed reads cannot be served.
    pub fn apply_raft_outputs(&mut self, outputs: Vec<RaftOutput>) -> StepReportResult<G, A, R> {
        self.reject_if_poisoned()?;
        self.apply_outputs(outputs, false, StepReportOptions::default())
    }

    /// Reports every membership fact this group has moved through and not yet
    /// handed back, without stepping anything.
    ///
    /// **The error-path companion of the report stream.** A report is the only
    /// way a membership transition leaves this group, and a step that fails
    /// returns no report — while the runtime has already appended, truncated,
    /// committed, or installed the configuration that moved. Call this after a
    /// failed step, and the transition arrives anyway; call it after a
    /// successful one, and it is empty because the report already carried it.
    ///
    /// A driver that routes this after *every* step outcome, `Err` included,
    /// has a zero-width loss window at its own boundary and needs no later
    /// successful step to rescue the fact.
    ///
    /// Not gated on poison, deliberately. Poison is exactly the state in which a
    /// caller most needs to know which replicas the cluster last authorized, the
    /// derivation touches neither the runtime's step path nor the state machine,
    /// and a group that will never apply again has still moved through whatever
    /// its runtime moved through.
    ///
    /// Events carry their observation point — the last log index and the commit
    /// index as they stand *now* — rather than the moment the runtime moved, for
    /// the reason [`crate::membership::MembershipEvent::EffectiveChanged`]
    /// gives: a truncation and a snapshot install have no configuration entry to
    /// name, so the index is where the log stands when the fact is observed.
    pub fn drain_membership_events(&mut self) -> Vec<MembershipEvent<G>> {
        let mut report = GroupStepReport::new(self.group_id.clone());
        self.record_membership_changes(&mut report);
        report.membership_events
    }

    /// Applies the outputs one runtime step released.
    ///
    /// Separate from [`RaftGroup::apply_raft_outputs`] only in that it carries
    /// whether the step that produced these outputs was a membership *request*,
    /// which is the one thing an unaddressed `RejectProposal` needs to be
    /// readable.
    pub(super) fn apply_stepped_outputs(
        &mut self,
        outputs: Vec<RaftOutput>,
        membership_request: bool,
        options: StepReportOptions,
    ) -> StepReportResult<G, A, R> {
        self.apply_outputs(outputs, membership_request, options)
    }

    /// Builds one report: queue the configurations, record, apply, complete
    /// reads, then report what the membership owes.
    ///
    /// The membership derivation runs last and takes no argument, because it
    /// compares against durable state rather than a pre-step snapshot. Every
    /// fallible statement above it therefore leaves the delta owed rather than
    /// consumed, which is what makes a failed step lossless in membership.
    ///
    /// **That argument covers the failures below the loop and not the ones
    /// inside it, which is why the configurations are queued first.** Applying,
    /// completing reads, and the readiness probe all run after the whole output
    /// vector has been walked, so the crossing queue is already full when they
    /// raise. Decoding does not: it runs once per `Apply`, inside the scan, and
    /// a payload the state machine refuses abandons the rest of the vector where
    /// it stands. A configuration behind that `Apply` was never visited, so
    /// nothing queued it and nothing owed it — and the endpoint comparison
    /// cannot stand in, because a commit that admits an identity and removes it
    /// again lands on the membership it started from.
    ///
    /// The pre-pass is infallible by construction: it matches one variant and
    /// pushes onto a queue. Nothing in it can fail, so there is no ordering
    /// question about what it leaves half-done. It also changes no reported
    /// order — `record_committed_configuration` never wrote into the report,
    /// only into the queue that `record_membership_changes` drains at the end —
    /// so this moves *when* the fact becomes owed and nothing else.
    pub(super) fn apply_outputs(
        &mut self,
        outputs: Vec<RaftOutput>,
        membership_request: bool,
        options: StepReportOptions,
    ) -> StepReportResult<G, A, R> {
        let mut report = GroupStepReport::new(self.group_id.clone());
        let mut apply_entries = Vec::new();

        self.queue_committed_configurations(&outputs);
        for output in outputs {
            self.record_raft_output(output, membership_request, &mut apply_entries, &mut report)?;
        }

        self.apply_entries(apply_entries, &mut report)?;
        self.complete_ready_reads(&mut report)?;
        self.record_membership_changes(&mut report);
        if options.include_metrics {
            report.metrics = Some(self.metrics());
        }
        Ok(report)
    }

    /// Records a quorum-confirmed read index and resolves its application floor.
    ///
    /// The floor is derived once, here, rather than on every later step for
    /// every pending read: that keeps the read table's per-step cost unchanged,
    /// and it makes the floor a fixed property of the barrier, so a caller
    /// polling toward [`crate::read::ReadEvent::FreshnessUnavailable`] sees a
    /// stable target that a later commit or compaction cannot move. It is
    /// computed before the pending entry is borrowed mutably.
    fn record_granted_read_index(&mut self, read_id: ReadId, read_index: LogIndex) {
        let application_floor = self.raft.committed_application_index_through(read_index);
        if let Some(pending) = self.pending_reads.get_mut(&read_id) {
            pending.granted = Some(GrantedReadIndex {
                read_index,
                application_floor,
            });
        }
    }

    #[allow(
        clippy::match_same_arms,
        reason = "an unaddressed rejection and an already-queued configuration are \
                  different facts that happen to need nothing here; merging them \
                  would hide which output each comment is about"
    )]
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
            } => self.record_appended_proposal(proposal_id, index, term, report),
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
            } => self.record_granted_read_index(read_id, read_index),
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
            // Already queued, by the infallible pre-pass `apply_outputs` runs
            // over the whole vector before this loop starts. Queueing it here
            // instead made the fact conditional on every earlier output having
            // been handled successfully, which an undecodable `Apply` at a lower
            // index is exactly what prevents. See `apply_outputs`.
            // Already queued, by the infallible pre-pass `apply_outputs` runs
            // over the whole vector before this loop starts. Queueing it here
            // instead made the fact conditional on every earlier output having
            // been handled successfully, which an undecodable `Apply` at a lower
            // index is exactly what prevents. See `apply_outputs`.
            RaftOutput::ConfigurationCommitted { .. } => {}
        }
        Ok(())
    }

    /// Records a local append, but only for a proposal this group is tracking.
    ///
    /// The runtime reports every tracked append it made, and a group adopted
    /// over a live runtime can be handed one for a proposal an earlier
    /// incarnation submitted. Reporting that would resolve a waiter this group
    /// never created.
    fn record_appended_proposal(
        &self,
        proposal_id: LocalProposalId,
        index: LogIndex,
        term: Term,
        report: &mut GroupStepReport<G, A::CommandResult>,
    ) {
        if self.pending_proposals.contains_key(&proposal_id) {
            report.proposal_events.push(ProposalEvent::Appended {
                local_proposal_id: proposal_id,
                index,
                term,
            });
        }
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
