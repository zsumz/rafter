#![allow(clippy::wildcard_imports)]

use super::*;

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
        self.reject_for_operation(group_id)?;
        let local_proposal_id = self
            .next_local_proposal_id()
            .map_err(ManagedOperationError::Write)?;
        let proposal = Proposal {
            local_proposal_id,
            client_request_id: options.client_request_id,
            command,
        };
        let report = match self
            .primary_group_mut()?
            .step(GroupInput::Proposal { proposal })
        {
            Ok(report) => report,
            Err(error) => {
                if let Some(write_error) =
                    self.poisoned_write_error_from_primary(local_proposal_id, options)
                {
                    self.publish_primary_metrics();
                    return Err(ManagedOperationError::Write(write_error));
                }
                if matches!(
                    &error,
                    GroupError::ProposalDidNotStart {
                        local_proposal_id: id
                    } if *id == local_proposal_id
                ) {
                    self.publish_primary_metrics();
                    return Err(ManagedOperationError::Write(write_unknown_outcome(
                        local_proposal_id,
                        options,
                        UnknownOutcomeReason::RuntimeDroppedProposal,
                    )));
                }
                return Err(error.into());
            }
        };
        let mut saw_local_append = report_has_local_append(local_proposal_id, &report);
        let outcome = observe_write_report(local_proposal_id, options, &report);
        self.route_report(report);
        if let Some(outcome) = outcome {
            self.publish_primary_metrics();
            return outcome;
        }
        for _ in 0..self.max_drive_steps {
            let dispatched = match self.dispatch_one() {
                Ok(dispatched) => dispatched,
                Err(error) => {
                    if let Some(write_error) =
                        self.poisoned_write_error_from_primary(local_proposal_id, options)
                    {
                        self.publish_primary_metrics();
                        return Err(ManagedOperationError::Write(write_error));
                    }
                    if saw_local_append {
                        // After local append, losing the driver path means we
                        // can no longer prove whether the entry later commits
                        // and applies, so surface client-facing uncertainty.
                        self.publish_primary_metrics();
                        return Err(ManagedOperationError::Write(write_unknown_outcome(
                            local_proposal_id,
                            options,
                            UnknownOutcomeReason::PostAppendDriverError,
                        )));
                    }
                    return Err(error);
                }
            };
            if let Some(report) = dispatched {
                saw_local_append |= report_has_local_append(local_proposal_id, &report);
                let outcome = observe_write_report(local_proposal_id, options, &report);
                self.route_report(report);
                if let Some(outcome) = outcome {
                    self.publish_primary_metrics();
                    return outcome;
                }
            } else {
                self.publish_primary_metrics();
                return Err(ManagedOperationError::Write(write_unknown_outcome(
                    local_proposal_id,
                    options,
                    UnknownOutcomeReason::EmptyNetwork,
                )));
            }
        }
        self.publish_primary_metrics();
        Err(ManagedOperationError::Write(write_unknown_outcome(
            local_proposal_id,
            options,
            UnknownOutcomeReason::DriveBoundReached,
        )))
    }
}

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
            write_error_from_rejection(reason.clone(), report.metrics.as_ref()),
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

pub(super) fn report_has_local_append<G, R>(
    local_proposal_id: LocalProposalId,
    report: &GroupStepReport<G, R>,
) -> bool {
    report.proposal_events.iter().any(|event| {
        matches!(
            event,
            ProposalEvent::Appended {
                local_proposal_id: id,
                ..
            } if *id == local_proposal_id
        )
    })
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

pub(super) fn write_error_from_rejection<G>(
    reason: ProposalRejection,
    metrics: Option<&RaftGroupMetrics<G>>,
) -> WriteError {
    match reason {
        ProposalRejection::NotLeader { term, .. } => WriteError::NotLeader {
            leader_hint: metrics.and_then(|metrics| metrics.leader_hint),
            term,
        },
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
