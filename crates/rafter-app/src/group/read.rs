use super::{
    max, Arc, BTreeSet, CompletedQueryRead, Debug, GroupError, GroupInput, GroupResult,
    GroupStepReport, LogIndex, PendingQueryRead, PendingRead, PersistedRaftRuntime, RaftGroup,
    RaftInput, ReadBarrier, ReadBarrierBeginReport, ReadBarrierBeginReportResult,
    ReadBarrierRequest, ReadConsistency, ReadEvent, ReadId, ReadIndexCancelReason,
    ReadIndexRejection, ReadOutcome, ReadOutcomeResult, ReadProof, ReadProofOutcome, ReadReport,
    ReadReportResult, ReadRequest, ReplicatedStateMachine, StateMachineOperation,
    StepReportOptions, StepReportResult,
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
    /// applies, snapshot events, membership events, leadership-transfer
    /// events, or metrics emitted while starting the barrier.
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
        Ok(self.begin_read_barrier(request)?.outcome)
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

    /// Drops local app-layer state for a pending helper read, a
    /// freshness-stalled helper read, or a completed helper-read proof.
    ///
    /// This does not send a Raft protocol cancellation to peers. If the kernel
    /// later emits a read-index result for the same local ID, the app layer
    /// ignores it because the local waiter state has been removed. Call this
    /// when abandoning a helper read that returned [`ReadOutcome::Pending`] or
    /// [`ReadOutcome::LinearizableFreshnessUnavailable`]. The `ReadId` remains
    /// consumed, so future read-index operations must use a strictly larger
    /// ID.
    ///
    /// Returns `true` when any state was removed.
    pub fn cancel_read(&mut self, read_id: ReadId) -> bool {
        self.remove_read_state(read_id)
    }

    /// Drops a completed helper-read proof without affecting active barriers.
    ///
    /// Use this when a caller received [`ReadOutcome::Pending`], later drove
    /// the group far enough for the proof to complete, but no longer intends to
    /// retry the same helper read to consume that proof. Dropping the cached
    /// proof does not make the submitted `ReadId` reusable.
    ///
    /// Returns `true` when a completed proof was removed.
    pub fn drop_completed_read(&mut self, read_id: ReadId) -> bool {
        self.completed_query_reads.remove(&read_id).is_some()
    }

    /// Attempts a synchronous state-machine read using the requested consistency
    /// mode, and returns the outcome plus the full step report generated while
    /// serving it.
    ///
    /// Local reads do not contact Raft, may be stale, and do not carry or consume
    /// `ReadId`s. A local read can return [`ReadOutcome::LocalFreshnessUnavailable`]
    /// when `min_applied_index` is above the local applied index; that outcome does
    /// not reserve read state. Linearizable reads use the same read-index barrier
    /// and pending-read table as [`RaftGroup::begin_read_barrier`]; callers that
    /// receive [`ReadOutcome::Pending`] should route the report's peer messages,
    /// continue driving normal group steps, then retry with the same [`ReadId`],
    /// freshness requirement, and context to consume the completed proof. Callers
    /// that receive [`ReadOutcome::LinearizableFreshnessUnavailable`] should also
    /// keep driving and retry with the same local read parameters, or call
    /// [`RaftGroup::cancel_read`] before abandoning the read. Once a linearizable
    /// read-index operation is submitted, that `ReadId` is consumed even if the
    /// caller cancels or drops local helper state. Rafter does not compare opaque
    /// query values. Lease reads are rejected until lease support is explicitly
    /// configured in this layer.
    ///
    /// The returned report is the complete record of the step this call ran: peer
    /// messages the caller must route, committed applies, proposal events, read
    /// events belonging to other barriers, snapshot events, membership events,
    /// leadership-transfer events, and the metrics snapshot. Those effects are
    /// produced whether this read completes, stalls, is rejected, or is canceled;
    /// the outcome value alone never carries them. Reads that consume an already
    /// completed proof, and every [`ReadRequest::Local`] read, do not step the
    /// runtime and return an empty report for this group.
    ///
    /// A terminal read event clears local waiter state, so a caller that keeps
    /// retrying after observing [`ReadEvent::Rejected`] or [`ReadEvent::Canceled`]
    /// in the report receives [`GroupError::NonMonotonicReadId`] rather than a
    /// second statement of the rejection. Check the read events of every report
    /// the caller records before retrying — a barrier is most often ended by
    /// the tick or delivery step that observes a leadership change, so its
    /// terminal event arrives in that step's report rather than in one this
    /// method returned.
    ///
    /// Every branch is refused with
    /// [`GroupError::AppliedIndexBelowSnapshotBoundary`] while this group's
    /// state machine sits below the runtime's snapshot boundary, including the
    /// two that never step the runtime — a retry that consumes an already
    /// completed proof, and every [`ReadRequest::Local`]. Such a state machine
    /// is missing acknowledged entries that no longer exist in any form this
    /// composition can reach, so serving a query from it is the loss rather
    /// than a stale answer, and no freshness argument makes it fresh.
    ///
    /// # Errors
    ///
    /// Returns a group error when the group is poisoned, the state machine is
    /// below the runtime's snapshot boundary, the request targets a different
    /// group, the runtime rejects the underlying read-index request, the state
    /// machine cannot report its applied index, or the state-machine read
    /// fails.
    pub fn read(&mut self, request: ReadRequest<G, A::Query>) -> ReadReportResult<G, A, R> {
        self.reject_if_poisoned()?;
        self.reject_if_below_snapshot_boundary()?;
        match request {
            ReadRequest::Local {
                group_id,
                query,
                min_applied_index,
            } => {
                if group_id != self.group_id {
                    return Err(GroupError::WrongGroup);
                }
                self.read_local(query, min_applied_index)
            }
            ReadRequest::Linearizable {
                group_id,
                read_id,
                query,
                min_applied_index,
                context,
            } => {
                if group_id != self.group_id {
                    return Err(GroupError::WrongGroup);
                }
                self.read_linearizable(read_id, query, min_applied_index, context)
            }
            ReadRequest::Lease {
                group_id,
                query: _,
                min_applied_index: _,
            } => {
                if group_id != self.group_id {
                    return Err(GroupError::WrongGroup);
                }
                Err(GroupError::UnsupportedReadConsistency {
                    consistency: ReadConsistency::LeaseRead,
                })
            }
        }
    }

    /// Attempts a synchronous state-machine read and returns only its immediate
    /// outcome.
    ///
    /// This outcome-only helper intentionally discards the co-emitted step report.
    /// It is lossless for [`ReadRequest::Local`], which never steps the runtime,
    /// and for a retry that consumes an already completed proof. For a
    /// [`ReadRequest::Linearizable`] read that starts a barrier it discards peer
    /// messages, applies, proposal events, other barriers' read events, snapshot
    /// events, membership events, leadership-transfer events, and metrics emitted
    /// while the barrier started. A discarded [`ReadEvent::Granted`] destroys the
    /// only copy of that barrier's proof, and a discarded snapshot chunk directive
    /// is a lost protocol effect the caller was responsible for delivering. Use
    /// [`RaftGroup::read`] unless this group holds no other waiters and the caller
    /// routes no peer traffic.
    ///
    /// # Errors
    ///
    /// As [`RaftGroup::read`].
    pub fn read_outcome(
        &mut self,
        request: ReadRequest<G, A::Query>,
    ) -> ReadOutcomeResult<G, A, R> {
        Ok(self.read(request)?.outcome)
    }
    pub(super) fn record_rejected_read(
        &mut self,
        read_id: ReadId,
        reason: ReadIndexRejection,
        report: &mut GroupStepReport<G, A::CommandResult>,
    ) {
        if !self.remove_read_state(read_id) {
            return;
        }
        report.read_events.push(ReadEvent::Rejected {
            read_id,
            reason,
            leader_hint: self.raft.leader_hint(),
        });
    }

    pub(super) fn record_canceled_read(
        &mut self,
        read_id: ReadId,
        reason: ReadIndexCancelReason,
        report: &mut GroupStepReport<G, A::CommandResult>,
    ) {
        if !self.remove_read_state(read_id) {
            return;
        }
        report.read_events.push(ReadEvent::Canceled {
            read_id,
            reason,
            leader_hint: self.raft.leader_hint(),
        });
    }

    pub(super) fn remove_read_state(&mut self, read_id: ReadId) -> bool {
        let pending = self.pending_reads.remove(&read_id).is_some();
        let pending_query = self.pending_query_reads.remove(&read_id).is_some();
        let completed_query = self.completed_query_reads.remove(&read_id).is_some();
        pending || pending_query || completed_query
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
    pub(super) fn read_local(
        &self,
        query: A::Query,
        min_applied_index: Option<LogIndex>,
    ) -> ReadReportResult<G, A, R> {
        let local_applied_index =
            self.app
                .applied_index()
                .map_err(|source| GroupError::StateMachine {
                    operation: StateMachineOperation::AppliedIndex,
                    source: Arc::new(source),
                })?;
        let required_applied_index = max(
            local_applied_index,
            min_applied_index.unwrap_or(local_applied_index),
        );
        if local_applied_index < required_applied_index {
            return Ok(
                self.unstepped_read_report(ReadOutcome::LocalFreshnessUnavailable {
                    required_applied_index,
                    local_applied_index,
                }),
            );
        }

        let barrier = ReadBarrier {
            required_applied_index,
            local_applied_index,
        };
        let result = self.read_state_machine(query, barrier)?;
        Ok(self.unstepped_read_report(ReadOutcome::Ready {
            result,
            proof: None,
        }))
    }

    pub(super) fn read_linearizable(
        &mut self,
        read_id: ReadId,
        query: A::Query,
        min_applied_index: Option<LogIndex>,
        context: Vec<u8>,
    ) -> ReadReportResult<G, A, R> {
        if let Some(completed) = self.completed_query_reads.get(&read_id) {
            if completed.min_applied_index != min_applied_index || completed.context != context {
                return Err(GroupError::DuplicateReadId { read_id });
            }
            let Some(completed) = self.completed_query_reads.remove(&read_id) else {
                return Err(GroupError::DuplicateReadId { read_id });
            };
            self.pending_query_reads.remove(&read_id);
            let outcome = self.read_with_proof(query, completed.proof)?;
            return Ok(self.unstepped_read_report(outcome));
        }

        if let Some(pending_query) = self.pending_query_reads.get(&read_id) {
            if pending_query.min_applied_index != min_applied_index
                || pending_query.context != context
            {
                return Err(GroupError::DuplicateReadId { read_id });
            }
            let outcome = self.try_complete_pending_query_read(read_id, query)?;
            return Ok(self.unstepped_read_report(outcome));
        }

        let request = ReadBarrierRequest {
            group_id: self.group_id.clone(),
            read_id,
            min_applied_index,
            context: context.clone(),
        };
        // Taken before the barrier starts, because the state-machine read below
        // can fail and discard the report the barrier's step produced.
        let mark = self.membership_report_mark();
        let ReadBarrierBeginReport {
            outcome: proof_outcome,
            report,
        } = self.begin_read_barrier(request)?;
        if matches!(
            proof_outcome,
            ReadProofOutcome::Pending { .. } | ReadProofOutcome::FreshnessUnavailable { .. }
        ) {
            self.pending_query_reads.insert(
                read_id,
                PendingQueryRead {
                    min_applied_index,
                    context,
                },
            );
        }
        let outcome = match self.read_outcome_from_proof_outcome(read_id, query, proof_outcome) {
            Ok(outcome) => outcome,
            Err(error) => {
                self.restore_membership_report_mark(mark);
                return Err(error);
            }
        };
        Ok(ReadReport { outcome, report })
    }

    /// Pairs an outcome with an empty report for a read that never stepped the
    /// runtime, so a caller routes the report unconditionally instead of
    /// branching on whether this particular read touched Raft.
    fn unstepped_read_report(
        &self,
        outcome: ReadOutcome<G, A::QueryResult>,
    ) -> ReadReport<G, A::QueryResult, A::CommandResult> {
        ReadReport {
            outcome,
            report: GroupStepReport::new(self.group_id.clone()),
        }
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

    pub(super) fn try_complete_pending_query_read(
        &mut self,
        read_id: ReadId,
        query: A::Query,
    ) -> ReadOutcomeResult<G, A, R> {
        let Some(pending) = self.pending_reads.get(&read_id).copied() else {
            self.pending_query_reads.remove(&read_id);
            return Ok(ReadOutcome::Pending {
                read_id,
                peer_messages: Vec::new(),
            });
        };
        let Some(granted) = pending.granted else {
            return Ok(ReadOutcome::Pending {
                read_id,
                peer_messages: Vec::new(),
            });
        };

        let local_applied_index =
            self.app
                .applied_index()
                .map_err(|source| GroupError::StateMachine {
                    operation: StateMachineOperation::AppliedIndex,
                    source: Arc::new(source),
                })?;
        let required_applied_index = match pending.min_applied_index {
            Some(min) => max(granted.application_floor, min),
            None => granted.application_floor,
        };
        if local_applied_index < required_applied_index {
            return Ok(ReadOutcome::LinearizableFreshnessUnavailable {
                read_id,
                required_applied_index,
                local_applied_index,
            });
        }

        self.pending_reads.remove(&read_id);
        self.pending_query_reads.remove(&read_id);
        let proof = ReadProof {
            group_id: self.group_id.clone(),
            issued_by: self.node_id,
            term: self.raft.current_term(),
            read_index: granted.read_index,
            required_applied_index,
            local_applied_index,
        };
        self.read_with_proof(query, proof)
    }

    pub(super) fn read_outcome_from_proof_outcome(
        &mut self,
        read_id: ReadId,
        query: A::Query,
        outcome: ReadProofOutcome<G>,
    ) -> ReadOutcomeResult<G, A, R> {
        match outcome {
            ReadProofOutcome::Granted { proof } => {
                self.pending_query_reads.remove(&read_id);
                self.completed_query_reads.remove(&read_id);
                self.read_with_proof(query, proof)
            }
            ReadProofOutcome::Pending {
                read_id,
                peer_messages,
            } => Ok(ReadOutcome::Pending {
                read_id,
                peer_messages,
            }),
            ReadProofOutcome::Rejected {
                read_id,
                reason,
                leader_hint,
            } => {
                self.pending_query_reads.remove(&read_id);
                self.completed_query_reads.remove(&read_id);
                Ok(ReadOutcome::Rejected {
                    read_id,
                    reason,
                    leader_hint,
                })
            }
            ReadProofOutcome::Canceled {
                read_id,
                reason,
                leader_hint,
            } => {
                self.pending_query_reads.remove(&read_id);
                self.completed_query_reads.remove(&read_id);
                Ok(ReadOutcome::Canceled {
                    read_id,
                    reason,
                    leader_hint,
                })
            }
            ReadProofOutcome::FreshnessUnavailable {
                read_id,
                required_applied_index,
                local_applied_index,
            } => Ok(ReadOutcome::LinearizableFreshnessUnavailable {
                read_id,
                required_applied_index,
                local_applied_index,
            }),
        }
    }

    pub(super) fn read_with_proof(
        &self,
        query: A::Query,
        proof: ReadProof<G>,
    ) -> ReadOutcomeResult<G, A, R> {
        let barrier = ReadBarrier {
            required_applied_index: proof.required_applied_index,
            local_applied_index: proof.local_applied_index,
        };
        let result = self.read_state_machine(query, barrier)?;
        Ok(ReadOutcome::Ready {
            result,
            proof: Some(proof),
        })
    }

    pub(super) fn read_state_machine(
        &self,
        query: A::Query,
        barrier: ReadBarrier,
    ) -> GroupResult<A, R, A::QueryResult> {
        self.app
            .read(query, barrier)
            .map_err(|source| GroupError::StateMachine {
                operation: StateMachineOperation::Read,
                source: Arc::new(source),
            })
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
