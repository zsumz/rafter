//! Stepping a group: one input in, one report out.
//!
//! Every entry point here is poison-checked and boundary-checked before it
//! reaches the runtime. The raw output pump and the membership drain sit
//! beside them for callers that drive the persisted runtime themselves.

use super::{
    Debug, GroupError, GroupInput, GroupStepReport, LeadershipTransferEvent, MembershipEvent,
    PersistedRaftRuntime, Proposal, RaftGroup, RaftInput, RaftOutput, ReplicatedStateMachine,
    StepReportOptions, StepReportResult,
};

impl<G, A, R> RaftGroup<G, A, R>
where
    G: Clone + Ord + Debug + Send + Sync + 'static,
    A: ReplicatedStateMachine,
    A::CommandResult: Clone,
    R: PersistedRaftRuntime,
{
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
}
