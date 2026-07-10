#![allow(clippy::wildcard_imports)]

use super::*;

struct BatchWriteState<R> {
    local_proposal_id: LocalProposalId,
    options: WriteOptions,
    saw_local_append: bool,
    outcome: Option<Result<WriteReceipt<R>, WriteError>>,
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
    pub(super) fn write(
        &mut self,
        group_id: &G,
        command: A::Command,
        options: WriteOptions,
    ) -> ManagedWriteResult<A, R> {
        let mut outcomes = self.write_batch(
            group_id,
            vec![WriteBatchEntry::with_options(command, options)],
        );
        match outcomes.pop() {
            Some(Ok(receipt)) => Ok(receipt),
            Some(Err(error)) => Err(ManagedOperationError::Write(error)),
            None => Err(ManagedOperationError::Write(
                WriteError::ManagedInvariantViolation {
                    message: "managed single write produced no batch outcome".to_owned(),
                },
            )),
        }
    }

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
            let error = error.into_write_error();
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
                let error = error.into_write_error();
                return repeat_write_error(write_count, &error);
            }
        };
        let report = match report {
            Ok(report) => report,
            Err(error) => return self.finish_failed_write_batch(states, error),
        };

        let rejection_leader_hint = self.rejection_leader_hint(&report);
        observe_batch_report(&mut states, &report, rejection_leader_hint);
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
                    let write_error = error.into_write_error();
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
                let rejection_leader_hint = self.rejection_leader_hint(&report);
                observe_batch_report(&mut states, &report, rejection_leader_hint);
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
        if !poisoned.is_empty() {
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
        } else if matches!(error, GroupError::ProposalDidNotStart { .. }) {
            complete_unresolved_writes(&mut states, |state| {
                write_unknown_outcome(
                    state.local_proposal_id,
                    state.options,
                    UnknownOutcomeReason::RuntimeDroppedProposal,
                )
            });
        } else {
            let write_error = write_error_from_group(error);
            complete_unresolved_writes(&mut states, |_| write_error.clone());
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

    fn rejection_leader_hint(
        &self,
        report: &GroupStepReport<G, A::CommandResult>,
    ) -> Option<NodeId> {
        let has_not_leader_rejection = report.proposal_events.iter().any(|event| {
            matches!(
                event,
                ProposalEvent::Rejected {
                    reason: ProposalRejection::NotLeader { .. },
                    ..
                }
            )
        });
        if !has_not_leader_rejection {
            return None;
        }
        report
            .metrics
            .as_ref()
            .and_then(|metrics| metrics.leader_hint)
            .or_else(|| {
                self.groups
                    .get(&self.primary_node_id)
                    .and_then(rafter_app::group::RaftGroup::leader_hint)
            })
    }
}

fn observe_batch_report<G, R>(
    states: &mut [BatchWriteState<R>],
    report: &GroupStepReport<G, R>,
    rejection_leader_hint: Option<NodeId>,
) where
    R: Clone,
{
    debug_assert!(states
        .windows(2)
        .all(|window| window[0].local_proposal_id < window[1].local_proposal_id));

    for event in &report.proposal_events {
        let Some(local_proposal_id) = proposal_event_local_id(event) else {
            continue;
        };
        let Ok(position) =
            states.binary_search_by_key(&local_proposal_id, |state| state.local_proposal_id)
        else {
            continue;
        };
        let state = &mut states[position];
        if state.outcome.is_some() {
            continue;
        }

        match event {
            ProposalEvent::Appended { .. } => {
                state.saw_local_append = true;
            }
            ProposalEvent::Applied {
                index,
                term,
                result,
                ..
            } => {
                state.outcome = Some(Ok(WriteReceipt {
                    index: *index,
                    term: *term,
                    result: result.clone(),
                }));
            }
            ProposalEvent::Rejected { reason, .. } => {
                state.outcome = Some(Err(write_error_from_rejection(
                    reason.clone(),
                    rejection_leader_hint,
                )));
            }
            ProposalEvent::UnknownOutcome {
                client_request_id,
                reason,
                ..
            } => {
                state.outcome = Some(Err(WriteError::UnknownOutcome {
                    local_proposal_id,
                    client_request_id: client_request_id.or(state.options.client_request_id),
                    reason: managed_unknown_reason_from_app(reason),
                }));
            }
            _ => {}
        }
    }
}

fn proposal_event_local_id<R>(event: &ProposalEvent<R>) -> Option<LocalProposalId> {
    match event {
        ProposalEvent::Appended {
            local_proposal_id, ..
        }
        | ProposalEvent::Applied {
            local_proposal_id, ..
        }
        | ProposalEvent::Rejected {
            local_proposal_id, ..
        }
        | ProposalEvent::UnknownOutcome {
            local_proposal_id, ..
        } => Some(*local_proposal_id),
        _ => None,
    }
}

fn write_batch_complete<R>(states: &[BatchWriteState<R>]) -> bool {
    states.iter().all(|state| state.outcome.is_some())
}

fn complete_unresolved_writes<R>(
    states: &mut [BatchWriteState<R>],
    error_for: impl Fn(&BatchWriteState<R>) -> WriteError,
) {
    for state in states {
        if state.outcome.is_none() {
            state.outcome = Some(Err(error_for(state)));
        }
    }
}

fn finish_write_batch<R>(
    states: Vec<BatchWriteState<R>>,
) -> Vec<Result<WriteReceipt<R>, WriteError>> {
    states
        .into_iter()
        .map(|state| match state.outcome {
            Some(outcome) => outcome,
            None => Err(WriteError::ManagedInvariantViolation {
                message: "managed write batch finished without an outcome".to_owned(),
            }),
        })
        .collect()
}

fn repeat_write_error<R>(
    count: usize,
    error: &WriteError,
) -> Vec<Result<WriteReceipt<R>, WriteError>> {
    (0..count).map(|_| Err(error.clone())).collect()
}

#[cfg(test)]
pub(super) fn observe_write_report<G, R, E, RE>(
    local_proposal_id: LocalProposalId,
    options: WriteOptions,
    report: &GroupStepReport<G, R>,
) -> Option<Result<WriteReceipt<R>, ManagedOperationError<E, RE>>>
where
    R: Clone,
    E: Debug,
    RE: Debug,
{
    report.proposal_events.iter().find_map(|event| match event {
        ProposalEvent::Applied {
            local_proposal_id: id,
            index,
            term,
            result,
        } if *id == local_proposal_id => Some(Ok(WriteReceipt {
            index: *index,
            term: *term,
            result: result.clone(),
        })),
        ProposalEvent::Rejected {
            local_proposal_id: id,
            reason,
        } if *id == local_proposal_id => Some(Err(ManagedOperationError::Write(
            write_error_from_rejection(
                reason.clone(),
                report
                    .metrics
                    .as_ref()
                    .and_then(|metrics| metrics.leader_hint),
            ),
        ))),
        ProposalEvent::UnknownOutcome {
            local_proposal_id: id,
            client_request_id,
            reason,
        } if *id == local_proposal_id => Some(Err(ManagedOperationError::Write(
            WriteError::UnknownOutcome {
                local_proposal_id,
                client_request_id: client_request_id.or(options.client_request_id),
                reason: managed_unknown_reason_from_app(reason),
            },
        ))),
        _ => None,
    })
}

pub(super) fn managed_unknown_reason_from_app(
    reason: &ProposalUnknownOutcomeReason,
) -> UnknownOutcomeReason {
    match reason {
        ProposalUnknownOutcomeReason::GroupPoisoned => UnknownOutcomeReason::GroupPoisoned,
        ProposalUnknownOutcomeReason::LocalProposalDropped { .. }
        | ProposalUnknownOutcomeReason::ProposalDidNotStart => {
            UnknownOutcomeReason::RuntimeDroppedProposal
        }
        _ => unknown_future_app_reason(),
    }
}

pub(super) fn unknown_future_app_reason() -> UnknownOutcomeReason {
    UnknownOutcomeReason::RuntimeDroppedProposal
}

pub(super) fn write_unknown_outcome(
    local_proposal_id: LocalProposalId,
    options: WriteOptions,
    reason: UnknownOutcomeReason,
) -> WriteError {
    WriteError::UnknownOutcome {
        local_proposal_id,
        client_request_id: options.client_request_id,
        reason,
    }
}

pub(super) fn write_error_from_rejection(
    reason: ProposalRejection,
    leader_hint: Option<NodeId>,
) -> WriteError {
    match reason {
        ProposalRejection::NotLeader { term, .. } => WriteError::NotLeader { leader_hint, term },
        ProposalRejection::PayloadTooLarge {
            payload_len,
            max_payload_len,
        } => WriteError::PayloadTooLarge {
            max: max_payload_len,
            actual: payload_len,
        },
        reason => WriteError::Rejected { reason },
    }
}

#[cfg(test)]
mod tests {
    use rafter::Term;

    use super::*;

    fn unknown_outcome_report(reason: ProposalUnknownOutcomeReason) -> GroupStepReport<(), ()> {
        GroupStepReport {
            group_id: (),
            peer_messages: Vec::new(),
            applied: Vec::new(),
            proposal_events: vec![ProposalEvent::UnknownOutcome {
                local_proposal_id: LocalProposalId(7),
                client_request_id: None,
                reason,
            }],
            read_events: Vec::new(),
            leadership_transfer_events: Vec::new(),
            snapshot_events: Vec::new(),
            membership_events: Vec::new(),
            metrics: None,
        }
    }

    fn assert_observed_unknown_reason(
        report: &GroupStepReport<(), ()>,
        expected_reason: UnknownOutcomeReason,
    ) {
        let observed = observe_write_report::<(), (), String, ()>(
            LocalProposalId(7),
            WriteOptions::default(),
            report,
        );

        match observed {
            Some(Err(ManagedOperationError::Write(WriteError::UnknownOutcome {
                reason, ..
            }))) => {
                assert_eq!(reason, expected_reason);
            }
            other => panic!("expected unknown outcome write error, got {other:?}"),
        }
    }

    #[test]
    fn observe_batch_report_maps_batch_events_without_per_state_scans() {
        let client_request_id = rafter_app::proposal::ClientRequestId {
            client_id: 7,
            sequence: 9,
        };
        let mut states = vec![
            BatchWriteState {
                local_proposal_id: LocalProposalId(7),
                options: WriteOptions {
                    client_request_id: Some(client_request_id),
                },
                saw_local_append: false,
                outcome: None,
            },
            BatchWriteState {
                local_proposal_id: LocalProposalId(8),
                options: WriteOptions::default(),
                saw_local_append: false,
                outcome: None,
            },
        ];
        let report = GroupStepReport {
            group_id: (),
            peer_messages: Vec::new(),
            applied: Vec::new(),
            proposal_events: vec![
                ProposalEvent::Appended {
                    local_proposal_id: LocalProposalId(7),
                    index: LogIndex(7),
                    term: Term(1),
                },
                ProposalEvent::Applied {
                    local_proposal_id: LocalProposalId(8),
                    index: LogIndex(8),
                    term: Term(1),
                    result: "done".to_owned(),
                },
                ProposalEvent::UnknownOutcome {
                    local_proposal_id: LocalProposalId(7),
                    client_request_id: None,
                    reason: ProposalUnknownOutcomeReason::LocalProposalDropped {
                        index: LogIndex(7),
                        term: Term(1),
                        reason: rafter::LocalProposalDropReason::LeadershipLost,
                    },
                },
            ],
            read_events: Vec::new(),
            leadership_transfer_events: Vec::new(),
            snapshot_events: Vec::new(),
            membership_events: Vec::new(),
            metrics: None,
        };

        observe_batch_report(&mut states, &report, None);

        assert!(states[0].saw_local_append);
        assert_eq!(
            states[0].outcome,
            Some(Err(WriteError::UnknownOutcome {
                local_proposal_id: LocalProposalId(7),
                client_request_id: Some(client_request_id),
                reason: UnknownOutcomeReason::RuntimeDroppedProposal,
            }))
        );
        assert_eq!(
            states[1].outcome,
            Some(Ok(WriteReceipt {
                index: LogIndex(8),
                term: Term(1),
                result: "done".to_owned(),
            }))
        );
    }

    #[test]
    fn observe_batch_report_uses_explicit_rejection_leader_hint() {
        let mut states: Vec<BatchWriteState<()>> = vec![BatchWriteState {
            local_proposal_id: LocalProposalId(7),
            options: WriteOptions::default(),
            saw_local_append: false,
            outcome: None,
        }];
        let report = GroupStepReport {
            group_id: (),
            peer_messages: Vec::new(),
            applied: Vec::new(),
            proposal_events: vec![ProposalEvent::Rejected {
                local_proposal_id: LocalProposalId(7),
                reason: ProposalRejection::NotLeader {
                    role: Role::Follower,
                    term: Term(3),
                    payload_len: 11,
                },
            }],
            read_events: Vec::new(),
            leadership_transfer_events: Vec::new(),
            snapshot_events: Vec::new(),
            membership_events: Vec::new(),
            metrics: None,
        };

        observe_batch_report(&mut states, &report, Some(NodeId(2)));

        assert_eq!(
            states[0].outcome,
            Some(Err(WriteError::NotLeader {
                leader_hint: Some(NodeId(2)),
                term: Term(3),
            }))
        );
    }

    #[test]
    fn observe_write_report_maps_local_proposal_dropped_unknown_reason() {
        let report = unknown_outcome_report(ProposalUnknownOutcomeReason::LocalProposalDropped {
            index: LogIndex(2),
            term: Term(1),
            reason: rafter::LocalProposalDropReason::LeadershipLost,
        });

        assert_observed_unknown_reason(&report, UnknownOutcomeReason::RuntimeDroppedProposal);
    }

    #[test]
    fn observe_write_report_maps_group_poisoned_unknown_reason() {
        let report = unknown_outcome_report(ProposalUnknownOutcomeReason::GroupPoisoned);

        assert_observed_unknown_reason(&report, UnknownOutcomeReason::GroupPoisoned);
    }
}
