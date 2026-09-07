//! Runtime output translation into one step report.
//!
//! Every output a runtime step released becomes exactly one reported fact or
//! one deliberate silence, ordered so that a step failing partway still owes
//! its membership delta rather than losing it. Nothing here delivers or
//! retries: routing the collected messages and chunks is the caller's work.

use super::{
    ApplyEntry, Debug, GrantedReadIndex, GroupResult, GroupStepReport, LeadershipTransferEvent,
    LocalProposalId, LogIndex, Message, NodeId, PeerEnvelope, PersistedRaftRuntime, ProposalEvent,
    RaftGroup, RaftOutput, ReadId, ReplicatedStateMachine, SnapshotEvent, StepReportOptions,
    StepReportResult, Term,
};

impl<G, A, R> RaftGroup<G, A, R>
where
    G: Clone + Ord + Debug + Send + Sync + 'static,
    A: ReplicatedStateMachine,
    A::CommandResult: Clone,
    R: PersistedRaftRuntime,
{
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
