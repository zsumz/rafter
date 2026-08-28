//! Serving a query: local, linearizable, and against a completed proof.
//!
//! The three paths differ only in what they must prove before the state
//! machine is asked. A read that consumes an already completed proof and
//! every local read never step the runtime, and say so with an empty
//! report rather than by returning a different shape.

use super::{
    max, Arc, Debug, GroupError, GroupStepReport, LogIndex, PendingQueryRead, PersistedRaftRuntime,
    RaftGroup, ReadBarrier, ReadBarrierBeginReport, ReadBarrierRequest, ReadId, ReadOutcome,
    ReadOutcomeResult, ReadProof, ReadProofOutcome, ReadReport, ReadReportResult,
    ReplicatedStateMachine, StateMachineOperation,
};

impl<G, A, R> RaftGroup<G, A, R>
where
    G: Clone + Ord + Debug + Send + Sync + 'static,
    A: ReplicatedStateMachine,
    A::CommandResult: Clone,
    R: PersistedRaftRuntime,
{
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
                self.restore_membership_report_mark(mark, &report);
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
}
