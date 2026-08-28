use super::{
    Arc, Debug, GroupError, GroupResult, GroupStepReport, PersistedRaftRuntime, RaftGroup,
    ReadBarrier, ReadConsistency, ReadEvent, ReadId, ReadIndexCancelReason, ReadIndexRejection,
    ReadOutcome, ReadOutcomeResult, ReadProof, ReadReport, ReadReportResult, ReadRequest,
    ReplicatedStateMachine, StateMachineOperation,
};

impl<G, A, R> RaftGroup<G, A, R>
where
    G: Clone + Ord + Debug + Send + Sync + 'static,
    A: ReplicatedStateMachine,
    A::CommandResult: Clone,
    R: PersistedRaftRuntime,
{
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
    /// events, leadership-transfer events, and metrics emitted while the barrier
    /// started. A discarded [`ReadEvent::Granted`] destroys the only copy of that
    /// barrier's proof, and a discarded snapshot chunk directive is a lost
    /// protocol effect the caller was responsible for delivering. Use
    /// [`RaftGroup::read`] unless this group holds no other waiters and the caller
    /// routes no peer traffic.
    ///
    /// **Membership is the one stream it does not discard**, for the reason
    /// [`RaftGroup::begin_proposal_outcome`] gives.
    ///
    /// # Errors
    ///
    /// As [`RaftGroup::read`].
    pub fn read_outcome(
        &mut self,
        request: ReadRequest<G, A::Query>,
    ) -> ReadOutcomeResult<G, A, R> {
        let mark = self.membership_report_mark();
        let ReadReport { outcome, report } = self.read(request)?;
        self.restore_membership_report_mark(mark, &report);
        Ok(outcome)
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
}
