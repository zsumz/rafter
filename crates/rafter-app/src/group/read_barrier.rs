//! Starting a read barrier, completing it, and reading its outcome.
//!
//! A barrier ends in whichever step observes its cause, so the completion
//! pass runs on every step rather than in the call that started it, and the
//! outcome is derived from the report that step produced.

use super::{
    max, Arc, BTreeSet, CompletedQueryRead, Debug, GroupError, GroupInput, GroupResult,
    GroupStepReport, PendingRead, PersistedRaftRuntime, RaftGroup, RaftInput,
    ReadBarrierBeginReport, ReadBarrierBeginReportResult, ReadBarrierRequest, ReadEvent, ReadId,
    ReadProof, ReadProofOutcome, ReplicatedStateMachine, StateMachineOperation, StepReportOptions,
    StepReportResult,
};

impl<G, A, R> RaftGroup<G, A, R>
where
    G: Clone + Ord + Debug + Send + Sync + 'static,
    A: ReplicatedStateMachine,
    A::CommandResult: Clone,
    R: PersistedRaftRuntime,
{
    pub(super) fn step_read_barrier_input(
        &mut self,
        request: &ReadBarrierRequest<G>,
        options: StepReportOptions,
    ) -> StepReportResult<G, A, R> {
        self.validate_read_barrier_request(request)?;
        let read_id = request.read_id;
        if self.read_id_is_active(read_id) {
            return Err(GroupError::DuplicateReadId { read_id });
        }
        self.reject_non_monotonic_read_id(read_id)?;
        self.pending_reads.insert(
            read_id,
            PendingRead {
                min_applied_index: request.min_applied_index,
                granted: None,
            },
        );
        self.last_seen_read_id = Some(read_id);
        let outputs = match self.raft.step(RaftInput::ReadIndex { read_id }) {
            Ok(outputs) => outputs,
            Err(error) => {
                self.pending_reads.remove(&read_id);
                return Err(GroupError::Runtime(error));
            }
        };
        self.apply_stepped_outputs(outputs, false, options)
    }

    /// Begins a read-index barrier and returns its immediate proof outcome.
    ///
    /// This outcome-only helper intentionally discards co-emitted report
    /// streams. Use [`RaftGroup::begin_read_barrier`] when callers must observe
    /// applies, snapshot events, leadership-transfer events, or metrics emitted
    /// while starting the barrier.
    ///
    /// **Membership is the one stream it does not discard**, for the reason
    /// [`RaftGroup::begin_proposal_outcome`] gives: the delta is put back and
    /// stays owed rather than being reported into a report no caller received.
    ///
    /// A [`ReadProofOutcome::Pending`] or
    /// [`ReadProofOutcome::FreshnessUnavailable`] result reserves `read_id`
    /// until the barrier is granted, rejected, canceled by the runtime, or
    /// removed with [`RaftGroup::cancel_read`]. Low-level callers should cancel
    /// the read before abandoning it. Submitted read-index IDs are consumed;
    /// canceling a local waiter does not make the `ReadId` reusable.
    ///
    /// # Errors
    ///
    /// Returns a group error when the group is poisoned, the request targets a
    /// different group, the runtime rejects the read-index input, or applying
    /// any synchronous Raft outputs fails.
    pub fn begin_read_barrier_outcome(
        &mut self,
        request: ReadBarrierRequest<G>,
    ) -> GroupResult<A, R, ReadProofOutcome<G>> {
        let mark = self.membership_report_mark();
        let ReadBarrierBeginReport { outcome, report } = self.begin_read_barrier(request)?;
        self.restore_membership_report_mark(mark, &report);
        Ok(outcome)
    }

    /// Begins a read-index barrier and returns its immediate proof outcome plus
    /// the full step report generated while starting it.
    ///
    /// Use this method when callers must observe co-emitted applies, snapshot
    /// events, membership events, leadership-transfer events, or metrics
    /// instead of only the read-proof convenience value.
    ///
    /// # Errors
    ///
    /// Returns a group error when the group is poisoned, the request targets a
    /// different group, the runtime rejects the read-index input, or applying
    /// any synchronous Raft outputs fails.
    pub fn begin_read_barrier(
        &mut self,
        request: ReadBarrierRequest<G>,
    ) -> ReadBarrierBeginReportResult<G, A, R> {
        self.reject_if_poisoned()?;
        let read_id = request.read_id;
        let report = self.step(GroupInput::ReadBarrier { request })?;
        let outcome = Self::read_outcome_from_report(read_id, &report);
        Ok(ReadBarrierBeginReport { outcome, report })
    }

    pub(super) fn complete_ready_reads(
        &mut self,
        report: &mut GroupStepReport<G, A::CommandResult>,
    ) -> GroupResult<A, R, ()> {
        let granted_reads = self
            .pending_reads
            .iter()
            .filter_map(|(read_id, pending)| {
                pending
                    .granted
                    .map(|granted| (*read_id, granted, pending.min_applied_index))
            })
            .collect::<Vec<_>>();
        if granted_reads.is_empty() {
            return Ok(());
        }

        let local_applied_index =
            self.app
                .applied_index()
                .map_err(|source| GroupError::StateMachine {
                    operation: StateMachineOperation::AppliedIndex,
                    source: Arc::new(source),
                })?;
        for (read_id, granted, min_applied_index) in granted_reads {
            let required_applied_index = match min_applied_index {
                Some(min) => max(granted.application_floor, min),
                None => granted.application_floor,
            };
            if local_applied_index >= required_applied_index {
                self.pending_reads.remove(&read_id);
                let proof = ReadProof {
                    group_id: self.group_id.clone(),
                    issued_by: self.node_id,
                    term: self.raft.current_term(),
                    read_index: granted.read_index,
                    required_applied_index,
                    local_applied_index,
                };
                if let Some(pending_query) = self.pending_query_reads.remove(&read_id) {
                    self.completed_query_reads.insert(
                        read_id,
                        CompletedQueryRead {
                            proof: proof.clone(),
                            min_applied_index: pending_query.min_applied_index,
                            context: pending_query.context,
                        },
                    );
                }
                report
                    .read_events
                    .push(ReadEvent::Granted { read_id, proof });
            } else {
                report.read_events.push(ReadEvent::FreshnessUnavailable {
                    read_id,
                    required_applied_index,
                    local_applied_index,
                });
            }
        }
        Ok(())
    }

    pub(super) fn read_id_is_active(&self, read_id: ReadId) -> bool {
        self.pending_reads.contains_key(&read_id)
            || self.pending_query_reads.contains_key(&read_id)
            || self.completed_query_reads.contains_key(&read_id)
    }

    pub(super) fn reject_non_monotonic_read_id(&self, read_id: ReadId) -> GroupResult<A, R, ()> {
        if let Some(last_seen_read_id) = self.last_seen_read_id {
            if read_id <= last_seen_read_id {
                return Err(GroupError::NonMonotonicReadId {
                    read_id,
                    last_seen_read_id,
                });
            }
        }
        Ok(())
    }

    pub(super) fn reserved_read_count(&self) -> usize {
        let mut read_ids = self.pending_reads.keys().copied().collect::<BTreeSet<_>>();
        read_ids.extend(self.pending_query_reads.keys().copied());
        read_ids.extend(self.completed_query_reads.keys().copied());
        read_ids.len()
    }

    pub(super) fn read_outcome_from_report(
        read_id: ReadId,
        report: &GroupStepReport<G, A::CommandResult>,
    ) -> ReadProofOutcome<G> {
        for event in &report.read_events {
            match event {
                ReadEvent::Granted {
                    read_id: event_read_id,
                    proof,
                } if *event_read_id == read_id => {
                    return ReadProofOutcome::Granted {
                        proof: proof.clone(),
                    };
                }
                ReadEvent::Rejected {
                    read_id: event_read_id,
                    reason,
                    leader_hint,
                } if *event_read_id == read_id => {
                    return ReadProofOutcome::Rejected {
                        read_id,
                        reason: *reason,
                        leader_hint: *leader_hint,
                    };
                }
                ReadEvent::Canceled {
                    read_id: event_read_id,
                    reason,
                    leader_hint,
                } if *event_read_id == read_id => {
                    return ReadProofOutcome::Canceled {
                        read_id,
                        reason: *reason,
                        leader_hint: *leader_hint,
                    };
                }
                ReadEvent::FreshnessUnavailable {
                    read_id: event_read_id,
                    required_applied_index,
                    local_applied_index,
                } if *event_read_id == read_id => {
                    return ReadProofOutcome::FreshnessUnavailable {
                        read_id,
                        required_applied_index: *required_applied_index,
                        local_applied_index: *local_applied_index,
                    };
                }
                _ => {}
            }
        }
        ReadProofOutcome::Pending {
            read_id,
            peer_messages: report.peer_messages.clone(),
        }
    }
}
