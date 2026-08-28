//! The managed single write, and where the batch behind it lives.
//!
//! One write is a one-entry batch, so this file is the adapter and
//! [`super::batch`] is the loop. [`super::batch_outcome`] reads a step's report
//! against the entries, and [`super::proposal_outcome`] says what one proposal's
//! outcome means to a client; both are re-exported or re-imported here so the
//! scenarios below read against the names they were written for.

#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

use super::*;

pub(super) use super::proposal_outcome::{
    managed_unknown_reason_from_app, write_error_from_rejection,
};

#[cfg(test)]
use super::{
    batch::BatchWriteState, batch_outcome::observe_batch_report,
    proposal_outcome::observe_write_report,
};

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
                    fate: WriteFate::Unresolved,
                    message: "managed single write produced no batch outcome".to_owned(),
                },
            )),
        }
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

        observe_batch_report(&mut states, &report);

        assert!(states[0].saw_local_append);
        assert!(
            matches!(
                &states[0].outcome,
                Some(Err(WriteError::UnknownOutcome {
                    local_proposal_id: LocalProposalId(7),
                    client_request_id: Some(actual),
                    reason: UnknownOutcomeReason::RuntimeDroppedProposal,
                })) if *actual == client_request_id
            ),
            "got {:?}",
            states[0].outcome
        );
        assert_eq!(
            states[1]
                .outcome
                .as_ref()
                .expect("the second entry resolved")
                .as_ref()
                .expect("the second entry applied"),
            &WriteReceipt {
                index: LogIndex(8),
                term: Term(1),
                result: "done".to_owned(),
            }
        );
    }

    #[test]
    fn observe_batch_report_uses_the_leader_hint_carried_by_the_rejection_event() {
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
                leader_hint: Some(NodeId(2)),
            }],
            read_events: Vec::new(),
            leadership_transfer_events: Vec::new(),
            snapshot_events: Vec::new(),
            membership_events: Vec::new(),
            // The hint travels with the event, so a report with no metrics
            // snapshot still redirects the client.
            metrics: None,
        };

        observe_batch_report(&mut states, &report);

        assert!(
            matches!(
                &states[0].outcome,
                Some(Err(WriteError::NotLeader {
                    leader_hint: Some(NodeId(2)),
                    term: Term(3),
                }))
            ),
            "got {:?}",
            states[0].outcome
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
