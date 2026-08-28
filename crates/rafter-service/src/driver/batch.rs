//! One proposal batch, driven to a terminal outcome for every entry.
//!
//! The in-memory driver owns every replica, so it moves frames itself until each
//! entry of the batch has an answer or the drive bound is reached. Every exit
//! resolves every entry: an unresolved write leaving this loop would be a client
//! waiting on a driver that has stopped driving.

#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

use super::batch_outcome::{
    complete_unresolved_writes, finish_write_batch, observe_batch_report, repeat_write_error,
    with_observed_fate, write_batch_complete,
};
use super::proposal_outcome::write_unknown_outcome;
use super::*;

pub(super) struct BatchWriteState<R> {
    pub(super) local_proposal_id: LocalProposalId,
    pub(super) options: WriteOptions,
    pub(super) saw_local_append: bool,
    pub(super) outcome: Option<Result<WriteReceipt<R>, WriteError>>,
}

struct PreparedWriteBatch<C, R> {
    states: Vec<BatchWriteState<R>>,
    proposals: Vec<Proposal<C>>,
}

impl<G, A, R> InMemoryRaftState<G, A, R>
where
    G: Clone + Ord + Debug + Send + Sync + 'static,
    A: ReplicatedStateMachine + Send + 'static,
    A::Command: Send + 'static,
    A::CommandResult: Clone + Send + 'static,
    A::Query: Clone + Send + 'static,
    A::QueryResult: Send + 'static,
    A::Error: Debug + Send + 'static,
    R: PersistedRaftRuntime + Send + 'static,
    R::Error: Debug + Send + 'static,
{
    pub(super) fn write_batch(
        &mut self,
        group_id: &G,
        writes: Vec<WriteBatchEntry<A::Command>>,
    ) -> Vec<Result<WriteReceipt<A::CommandResult>, WriteError>> {
        if writes.is_empty() {
            return Vec::new();
        }

        let write_count = writes.len();
        if let Err(error) = self.reject_for_operation(group_id) {
            // Refused before any group was stepped, so the commands were
            // observably not appended.
            let error = error.into_write_error(WriteFate::NotAppended);
            return repeat_write_error(write_count, &error);
        }
        let PreparedWriteBatch {
            mut states,
            proposals,
        } = match self.prepare_write_batch(writes) {
            Ok(batch) => batch,
            Err(error) => return repeat_write_error(write_count, &error),
        };

        let report = match self.primary_group_mut() {
            Ok(group) => group.step_with_options(
                GroupInput::ProposalBatch { proposals },
                StepReportOptions::without_metrics(),
            ),
            Err(error) => {
                let error = error.into_write_error(WriteFate::NotAppended);
                return repeat_write_error(write_count, &error);
            }
        };
        let report = match report {
            Ok(report) => report,
            Err(error) => return self.finish_failed_write_batch(states, error),
        };

        observe_batch_report(&mut states, &report);
        self.route_report(report);
        if write_batch_complete(&states) {
            self.publish_primary_metrics();
            return finish_write_batch(states);
        }

        for _ in 0..self.max_drive_steps {
            let dispatched = match self.dispatch_one() {
                Ok(dispatched) => dispatched,
                Err(error) => {
                    let poisoned = self.poisoned_write_errors_from_primary_batch(&states);
                    // Entries with no observed append are the ones this error
                    // describes, and for them the refusal was observed.
                    let write_error = error.into_write_error(WriteFate::NotAppended);
                    complete_unresolved_writes(&mut states, |state| {
                        poisoned
                            .get(&state.local_proposal_id)
                            .cloned()
                            .unwrap_or_else(|| {
                                if state.saw_local_append {
                                    write_unknown_outcome(
                                        state.local_proposal_id,
                                        state.options,
                                        UnknownOutcomeReason::PostAppendDriverError,
                                    )
                                } else {
                                    write_error.clone()
                                }
                            })
                    });
                    self.publish_primary_metrics();
                    return finish_write_batch(states);
                }
            };
            if let Some(report) = dispatched {
                observe_batch_report(&mut states, &report);
                self.route_report(report);
                if write_batch_complete(&states) {
                    self.publish_primary_metrics();
                    return finish_write_batch(states);
                }
            } else {
                complete_unresolved_writes(&mut states, |state| {
                    write_unknown_outcome(
                        state.local_proposal_id,
                        state.options,
                        UnknownOutcomeReason::EmptyNetwork,
                    )
                });
                self.publish_primary_metrics();
                return finish_write_batch(states);
            }
        }
        complete_unresolved_writes(&mut states, |state| {
            write_unknown_outcome(
                state.local_proposal_id,
                state.options,
                UnknownOutcomeReason::DriveBoundReached,
            )
        });
        self.publish_primary_metrics();
        finish_write_batch(states)
    }

    fn prepare_write_batch(
        &mut self,
        writes: Vec<WriteBatchEntry<A::Command>>,
    ) -> Result<PreparedWriteBatch<A::Command, A::CommandResult>, WriteError> {
        let local_proposal_ids = self.reserve_local_proposal_ids(writes.len())?;
        let mut states = Vec::with_capacity(writes.len());
        let mut proposals = Vec::with_capacity(writes.len());
        for (local_proposal_id, write) in local_proposal_ids.into_iter().zip(writes) {
            states.push(BatchWriteState {
                local_proposal_id,
                options: write.options,
                saw_local_append: false,
                outcome: None,
            });
            proposals.push(Proposal {
                local_proposal_id,
                client_request_id: write.options.client_request_id,
                command: write.command,
            });
        }
        Ok(PreparedWriteBatch { states, proposals })
    }

    fn finish_failed_write_batch(
        &mut self,
        mut states: Vec<BatchWriteState<A::CommandResult>>,
        error: GroupError<A::Error, R::Error>,
    ) -> Vec<Result<WriteReceipt<A::CommandResult>, WriteError>> {
        let poisoned = self.poisoned_write_errors_from_primary_batch(&states);
        if poisoned.is_empty() {
            // One failure, one error, but a fate per entry: `saw_local_append`
            // is the observation, and it can differ across a batch.
            let write_error = write_error_from_group(error, WriteFate::NotAppended);
            complete_unresolved_writes(&mut states, |state| {
                with_observed_fate(&write_error, state.saw_local_append)
            });
        } else {
            complete_unresolved_writes(&mut states, |state| {
                poisoned
                    .get(&state.local_proposal_id)
                    .cloned()
                    .unwrap_or_else(|| {
                        write_unknown_outcome(
                            state.local_proposal_id,
                            state.options,
                            UnknownOutcomeReason::GroupPoisoned,
                        )
                    })
            });
        }
        self.publish_primary_metrics();
        finish_write_batch(states)
    }

    fn poisoned_write_errors_from_primary_batch<T>(
        &mut self,
        states: &[BatchWriteState<T>],
    ) -> BTreeMap<LocalProposalId, WriteError> {
        let Some(group) = self.groups.get_mut(&self.primary_node_id) else {
            return BTreeMap::new();
        };
        if !group.poisoned_waiters().proposals.iter().any(|(id, _)| {
            states
                .iter()
                .any(|state| state.local_proposal_id == *id && state.outcome.is_none())
        }) {
            return BTreeMap::new();
        }
        let waiters = group.drain_poisoned_waiters();
        let mut proposal_waiters = waiters.proposals.into_iter().collect::<BTreeMap<_, _>>();
        states
            .iter()
            .filter_map(|state| {
                proposal_waiters
                    .remove(&state.local_proposal_id)
                    .map(|client_request_id| {
                        (
                            state.local_proposal_id,
                            WriteError::UnknownOutcome {
                                local_proposal_id: state.local_proposal_id,
                                client_request_id: client_request_id
                                    .or(state.options.client_request_id),
                                reason: UnknownOutcomeReason::GroupPoisoned,
                            },
                        )
                    })
            })
            .collect()
    }
}
