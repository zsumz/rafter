#![allow(clippy::wildcard_imports)]

mod support;

use std::sync::{Arc, Mutex};

use rafter_runtime::RaftRuntimeError;
use rafter_runtime_api::PersistedRaftRuntime;

use support::*;

#[test]
fn in_memory_driver_rejects_waiters_after_shutdown() {
    let driver = elected_driver();
    let handle = driver.handle();

    block_on(handle.shutdown()).expect("shutdown succeeds");

    let write = block_on(handle.write(("alpha".to_owned(), "one".to_owned())))
        .expect_err("a closed driver refuses new writes");
    let read = block_on(handle.read("alpha".to_owned(), ReadConsistency::Linearizable))
        .expect_err("a closed driver refuses new reads");
    let transfer = block_on(handle.transfer_leadership(NodeId(2)))
        .expect_err("a closed driver refuses new transfers");

    assert!(matches!(write, WriteError::ShuttingDown), "got {write:?}");
    assert_eq!(
        write.fate(),
        WriteFate::NotAppended,
        "a refused write never reached the log"
    );
    assert!(matches!(read, ReadError::ShuttingDown), "got {read:?}");
    assert!(
        matches!(transfer, TransferLeadershipError::ShuttingDown),
        "got {transfer:?}"
    );
}

#[test]
fn in_memory_driver_resolves_waiters_when_apply_poisons_group() {
    let driver = KvDriver::new_elected(
        NodeId(1),
        vec![group_with_app(
            1,
            &[],
            3,
            KvStateMachine {
                fail_apply: true,
                ..KvStateMachine::default()
            },
        )],
    )
    .expect("single-node primary elects");
    let handle = driver.handle();

    let write = block_on(handle.write(("alpha".to_owned(), "one".to_owned())))
        .expect_err("an apply failure poisons the group");

    assert!(
        matches!(
            write,
            WriteError::UnknownOutcome {
                local_proposal_id: LocalProposalId(1),
                client_request_id: None,
                reason: UnknownOutcomeReason::GroupPoisoned,
            }
        ),
        "got {write:?}"
    );
    assert!(
        write.fate().may_commit(),
        "the entry was appended before the apply failed"
    );

    let read = block_on(handle.read("alpha".to_owned(), ReadConsistency::Linearizable))
        .expect_err("a poisoned group refuses reads");
    let ReadError::Poisoned { cause, .. } = &read else {
        panic!("expected a poisoned read, got {read:?}");
    };
    // The app layer kept the state machine's own error beside the health state,
    // so the poison reports what broke rather than a rendered reason alone.
    assert_eq!(
        cause
            .as_ref()
            .and_then(ErrorCause::downcast_ref::<KvStateMachineError>),
        Some(&KvStateMachineError::Apply)
    );
}

#[test]
fn in_memory_driver_write_batch_submits_one_runtime_batch_and_preserves_order() {
    let stats = Arc::new(Mutex::new(BatchRuntimeStats::default()));
    let group = RaftGroup::new(
        (),
        NodeId(1),
        BatchRecordingRuntime {
            stats: stats.clone(),
            next_index: LogIndex(1),
        },
        KvStateMachine::default(),
    );
    let driver = InMemoryRaftDriver::new(NodeId(1), vec![group]).expect("driver builds");

    let outcomes = block_on(driver.write_batch(
        (),
        vec![
            WriteBatchEntry::new(("alpha".to_owned(), "one".to_owned())),
            WriteBatchEntry::new(("beta".to_owned(), "two".to_owned())),
        ],
    ));

    let receipts = outcomes
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("both writes commit and apply");
    assert_eq!(
        receipts,
        vec![
            WriteReceipt {
                index: LogIndex(1),
                term: Term(1),
                result: None,
            },
            WriteReceipt {
                index: LogIndex(2),
                term: Term(1),
                result: None,
            },
        ]
    );
    let stats = stats.lock().expect("stats lock");
    assert!(stats.step_batches.is_empty());
    assert_eq!(stats.proposal_batches.len(), 1);
    assert_eq!(
        stats.proposal_batches[0],
        vec![
            ClientProposalInput {
                proposal_id: Some(LocalProposalId(1)),
                payload: b"alpha\none".to_vec(),
            },
            ClientProposalInput {
                proposal_id: Some(LocalProposalId(2)),
                payload: b"beta\ntwo".to_vec(),
            },
        ]
    );
}

#[test]
fn in_memory_driver_write_batch_reserves_local_ids_all_or_nothing() {
    let stats = Arc::new(Mutex::new(BatchRuntimeStats::default()));
    let mut group = RaftGroup::new(
        (),
        NodeId(1),
        BatchRecordingRuntime {
            stats: stats.clone(),
            next_index: LogIndex(1),
        },
        KvStateMachine::default(),
    );
    group
        .begin_proposal_outcome(Proposal {
            local_proposal_id: LocalProposalId(u64::MAX - 1),
            client_request_id: None,
            command: ("manual".to_owned(), "one".to_owned()),
        })
        .expect("manual proposal seeds adopted watermark");
    let batches_before_driver = stats.lock().expect("stats lock").step_batches.len();
    let driver = InMemoryRaftDriver::new(NodeId(1), vec![group]).expect("driver builds");

    let exhausted = block_on(driver.write_batch(
        (),
        vec![
            WriteBatchEntry::new(("alpha".to_owned(), "one".to_owned())),
            WriteBatchEntry::new(("beta".to_owned(), "two".to_owned())),
        ],
    ));

    assert!(
        exhausted
            .iter()
            .all(|outcome| matches!(outcome, Err(WriteError::LocalProposalIdExhausted))),
        "got {exhausted:?}"
    );
    assert_eq!(exhausted.len(), 2);
    assert_eq!(
        stats.lock().expect("stats lock").step_batches.len(),
        batches_before_driver,
        "failed reservation must not submit a runtime batch"
    );
    assert!(
        stats
            .lock()
            .expect("stats lock")
            .proposal_batches
            .is_empty(),
        "failed reservation must not submit a proposal batch"
    );

    let last_id_outcome = block_on(driver.write_batch(
        (),
        vec![WriteBatchEntry::new(("last".to_owned(), "ok".to_owned()))],
    ));

    assert!(matches!(last_id_outcome.as_slice(), [Ok(_)]));
    let stats = stats.lock().expect("stats lock");
    assert_eq!(stats.step_batches.len(), batches_before_driver);
    assert_eq!(stats.proposal_batches.len(), 1);
    assert_eq!(
        stats.proposal_batches[0],
        vec![ClientProposalInput {
            proposal_id: Some(LocalProposalId(u64::MAX)),
            payload: b"last\nok".to_vec(),
        }]
    );
}

#[test]
fn in_memory_driver_reports_not_leader_before_election() {
    let driver = KvDriver::new(NodeId(1), groups()).expect("driver builds");
    let handle = driver.handle();

    let error = block_on(handle.write(("alpha".to_owned(), "one".to_owned())))
        .expect_err("follower rejects writes");

    assert!(matches!(
        error,
        WriteError::NotLeader {
            leader_hint: None,
            ..
        }
    ));
}

/// The managed write path steps with metrics disabled, so a rejection observed
/// there has no metrics snapshot to borrow a redirect from. The hint has to
/// travel on the event or the client is told "not leader" with nowhere to go.
#[test]
fn write_rejection_reports_a_leader_hint_when_metrics_are_disabled() {
    let driver = scripted_write_driver(ScriptedWriteMode::RejectNotLeader);
    let handle = driver.handle();

    let error = block_on(handle.write(("alpha".to_owned(), "one".to_owned())))
        .expect_err("a follower refuses the write");

    assert!(
        matches!(
            error,
            WriteError::NotLeader {
                leader_hint: Some(NodeId(2)),
                term: Term(1),
            }
        ),
        "got {error:?}"
    );
}

#[derive(Debug, Default)]
struct BatchRuntimeStats {
    step_batches: Vec<Vec<RaftInput>>,
    proposal_batches: Vec<Vec<ClientProposalInput>>,
}

struct BatchRecordingRuntime {
    stats: Arc<Mutex<BatchRuntimeStats>>,
    next_index: LogIndex,
}

impl PersistedRaftRuntime for BatchRecordingRuntime {
    type Error = RaftRuntimeError;

    fn id(&self) -> NodeId {
        NodeId(1)
    }

    fn leader_hint(&self) -> Option<NodeId> {
        Some(NodeId(1))
    }

    fn role(&self) -> Role {
        Role::Leader
    }

    fn current_term(&self) -> Term {
        Term(1)
    }

    fn commit_index(&self) -> LogIndex {
        self.last_recorded_index()
    }

    fn last_log_index(&self) -> LogIndex {
        self.last_recorded_index()
    }

    fn snapshot_index(&self) -> LogIndex {
        LogIndex::ZERO
    }

    /// Every index this fake records is an application proposal, and it
    /// commits each one as it records it, so the floor for a bound is the
    /// highest recorded index at or below it.
    fn committed_application_index_through(&self, index: LogIndex) -> LogIndex {
        std::cmp::min(index, self.last_recorded_index())
    }

    fn membership(&self) -> MembershipConfig {
        MembershipConfig::stable(
            MembershipSet::new(vec![NodeId(1)], Vec::new()).expect("scripted membership is valid"),
        )
    }

    fn replication(&self) -> Vec<ReplicationProgress> {
        Vec::new()
    }

    fn step(&mut self, input: RaftInput) -> Result<Vec<RaftOutput>, RaftRuntimeError> {
        self.step_batch(vec![input])
    }

    fn step_proposal_batch(
        &mut self,
        proposals: Vec<ClientProposalInput>,
    ) -> Result<Vec<RaftOutput>, RaftRuntimeError> {
        self.stats
            .lock()
            .expect("stats lock")
            .proposal_batches
            .push(proposals.clone());

        let mut outputs = Vec::new();
        for proposal in proposals {
            if let Some(proposal_id) = proposal.proposal_id {
                let index = self.next_index;
                self.next_index = self.next_index.next();
                outputs.push(RaftOutput::LocalProposalAppended {
                    proposal_id,
                    index,
                    term: Term(1),
                });
                outputs.push(RaftOutput::Apply {
                    index,
                    term: Term(1),
                    payload: SharedPayload::from(proposal.payload),
                    local_proposal_id: Some(proposal_id),
                });
            }
        }
        Ok(outputs)
    }

    fn step_batch(&mut self, inputs: Vec<RaftInput>) -> Result<Vec<RaftOutput>, RaftRuntimeError> {
        self.stats
            .lock()
            .expect("stats lock")
            .step_batches
            .push(inputs.clone());

        let mut outputs = Vec::new();
        for input in inputs {
            if let RaftInput::TrackedClientProposal {
                proposal_id,
                payload,
            } = input
            {
                let index = self.next_index;
                self.next_index = self.next_index.next();
                outputs.push(RaftOutput::LocalProposalAppended {
                    proposal_id,
                    index,
                    term: Term(1),
                });
                outputs.push(RaftOutput::Apply {
                    index,
                    term: Term(1),
                    payload: SharedPayload::from(payload),
                    local_proposal_id: Some(proposal_id),
                });
            }
        }
        Ok(outputs)
    }

    fn term_at_index(&self, index: LogIndex) -> Option<Term> {
        (index <= self.last_log_index()).then_some(Term(1))
    }
}

impl BatchRecordingRuntime {
    const fn last_recorded_index(&self) -> LogIndex {
        LogIndex(self.next_index.0.saturating_sub(1))
    }
}

#[test]
fn in_memory_driver_reports_proposal_id_exhaustion_after_max() {
    let mut adopted = group(1, &[], 3);
    adopted
        .begin_proposal_outcome(Proposal {
            local_proposal_id: LocalProposalId(u64::MAX - 1),
            client_request_id: None,
            command: ("manual".to_owned(), "one".to_owned()),
        })
        .expect("manual proposal consumes the penultimate local proposal id");
    let driver = KvDriver::new_elected(NodeId(1), vec![adopted])
        .expect("quiescent manually driven group is adoptable");
    let handle = driver.handle();

    block_on(handle.write(("last".to_owned(), "ok".to_owned())))
        .expect("last proposal ID may be used once");
    let error = block_on(handle.write(("again".to_owned(), "no".to_owned())))
        .expect_err("the local proposal id space is spent");

    assert!(
        matches!(error, WriteError::LocalProposalIdExhausted),
        "got {error:?}"
    );
    assert_eq!(
        error.fate(),
        WriteFate::NotAppended,
        "an id the driver never issued cannot have been proposed"
    );
}

#[test]
fn in_memory_driver_bounds_pending_writes_and_publishes_metrics() {
    let driver = scripted_write_driver(ScriptedWriteMode::AppendThenCycle);
    let handle = driver.handle();

    let error = block_on(handle.write(("alpha".to_owned(), "one".to_owned())))
        .expect_err("the drive bound is reached before a terminal result");

    assert!(
        matches!(
            error,
            WriteError::UnknownOutcome {
                local_proposal_id: LocalProposalId(1),
                client_request_id: None,
                reason: UnknownOutcomeReason::DriveBoundReached,
            }
        ),
        "got {error:?}"
    );
    assert_eq!(
        handle
            .metrics()
            .expect("metrics")
            .current()
            .pending_proposals,
        1,
        "bounded unknown outcome publishes pending proposal metrics"
    );
}

#[test]
fn in_memory_driver_publishes_metrics_for_empty_network_unknown_outcome() {
    let driver = scripted_write_driver(ScriptedWriteMode::AppendThenIdle);
    let handle = driver.handle();

    let error = block_on(handle.write(("alpha".to_owned(), "one".to_owned())))
        .expect_err("nothing is left to drive");

    assert!(
        matches!(
            error,
            WriteError::UnknownOutcome {
                local_proposal_id: LocalProposalId(1),
                client_request_id: None,
                reason: UnknownOutcomeReason::EmptyNetwork,
            }
        ),
        "got {error:?}"
    );
    assert_eq!(
        handle
            .metrics()
            .expect("metrics")
            .current()
            .pending_proposals,
        1,
        "empty-network unknown outcome publishes pending proposal metrics"
    );
}

#[test]
fn in_memory_driver_maps_post_append_dispatch_error_to_unknown_outcome() {
    let driver = scripted_write_driver(ScriptedWriteMode::AppendThenMissingNode);
    let handle = driver.handle();

    let error = block_on(handle.write(("alpha".to_owned(), "one".to_owned())))
        .expect_err("routing fails after the local append");

    assert!(
        matches!(
            error,
            WriteError::UnknownOutcome {
                local_proposal_id: LocalProposalId(1),
                client_request_id: None,
                reason: UnknownOutcomeReason::PostAppendDriverError,
            }
        ),
        "got {error:?}"
    );
    assert_eq!(
        handle
            .metrics()
            .expect("metrics")
            .current()
            .pending_proposals,
        1,
        "post-append unknown outcome publishes pending proposal metrics"
    );
}

/// The safety case. An entry that reached the local log and then hit a failure
/// must never be reported as refused: it is on disk, and a node reopened over
/// the same durable log can still replicate and commit it under a later
/// incarnation. A driver that derived the fate from the category would fail
/// this, because the category here is a routing failure.
#[test]
fn an_apply_failure_after_a_local_append_is_not_reported_as_not_appended() {
    let driver = scripted_write_driver(ScriptedWriteMode::AppendThenMissingNode);
    let handle = driver.handle();

    let error = block_on(handle.write(("alpha".to_owned(), "one".to_owned())))
        .expect_err("routing fails after the local append");

    assert_eq!(error.fate(), WriteFate::Unresolved);
    assert!(
        error.fate().may_commit(),
        "an appended entry may still commit under a later incarnation"
    );
}

/// The replacement for a test that pinned a rendered `RaftRuntimeError`
/// message as an exact string. The fact it always meant to assert — that a
/// pre-append runtime failure is reported as a runtime failure and could not
/// have committed — is two typed assertions now.
#[test]
fn a_pre_append_runtime_error_preserves_the_runtime_error_and_reports_not_appended() {
    let driver = scripted_write_driver(ScriptedWriteMode::PreAppendRuntimeError);
    let handle = driver.handle();

    let error = block_on(handle.write(("alpha".to_owned(), "one".to_owned())))
        .expect_err("the runtime refuses before appending");

    assert_eq!(error.kind(), WriteErrorKind::Storage);
    let WriteError::Storage { cause, .. } = &error else {
        panic!("expected a storage error, got {error:?}");
    };
    assert!(
        matches!(
            cause.downcast_ref::<RaftRuntimeError>(),
            Some(RaftRuntimeError::LogPrefixDiverged { index: LogIndex(1) })
        ),
        "the runtime's own error survives the boundary, got {cause:?}"
    );
    assert_eq!(
        error.fate(),
        WriteFate::NotAppended,
        "the driver observed no append, so the command could not have committed"
    );
    assert_eq!(
        handle
            .metrics()
            .expect("metrics")
            .current()
            .pending_proposals,
        0,
        "pre-append runtime error does not leak pending proposal state"
    );
}

/// The two-boundary case, and the one the state-machine error bound exists for:
/// an application's own error type survives from the state machine, through the
/// group, through the driver, to the client.
#[test]
fn a_state_machine_failure_reaches_the_client_as_its_own_type() {
    let driver = KvDriver::new_elected(
        NodeId(1),
        vec![group_with_app(
            1,
            &[],
            3,
            KvStateMachine {
                fail_apply: true,
                ..KvStateMachine::default()
            },
        )],
    )
    .expect("single-node primary elects");
    let handle = driver.handle();

    // The write itself resolves as an unknown outcome, because the entry
    // appended before the apply failed. The typed error is on the group.
    let _ = block_on(handle.write(("alpha".to_owned(), "one".to_owned())))
        .expect_err("an apply failure poisons the group");
    let error = block_on(handle.write(("beta".to_owned(), "two".to_owned())))
        .expect_err("a poisoned group refuses later writes");

    assert_eq!(error.kind(), WriteErrorKind::Poisoned);
    let WriteError::Poisoned { cause, .. } = &error else {
        panic!("expected a poisoned write, got {error:?}");
    };
    assert_eq!(
        cause
            .as_ref()
            .and_then(ErrorCause::downcast_ref::<KvStateMachineError>),
        Some(&KvStateMachineError::Apply),
        "the client downcasts to the state machine's own type"
    );
    assert_eq!(
        std::error::Error::source(&error)
            .expect("the preserved cause is the error's source")
            .to_string(),
        "apply failed"
    );
}

/// The old mapping folded six state-machine operations into two variants and
/// got one wrong: encoding a command was reported as a storage failure, and
/// encoding a command touches no storage.
#[test]
fn a_state_machine_error_reports_the_operation_that_surfaced_it() {
    let driver = KvDriver::new_elected(
        NodeId(1),
        vec![group_with_app(
            1,
            &[],
            3,
            KvStateMachine {
                fail_encode: true,
                ..KvStateMachine::default()
            },
        )],
    )
    .expect("single-node primary elects");
    let handle = driver.handle();

    let error = block_on(handle.write(("alpha".to_owned(), "one".to_owned())))
        .expect_err("the state machine cannot encode the command");

    assert_eq!(error.kind(), WriteErrorKind::StateMachine);
    assert!(
        matches!(
            error,
            WriteError::StateMachine {
                operation: StateMachineOperation::EncodeCommand,
                ..
            }
        ),
        "got {error:?}"
    );
    assert_eq!(
        error.fate(),
        WriteFate::NotAppended,
        "a command that could not be encoded was never proposed"
    );
}

/// The case both in-tree drivers got wrong: a write addressed to a group the
/// driver does not own was reported as a transport failure, which is both the
/// wrong category and — because a transport failure may have been delivered —
/// the wrong fate.
#[test]
fn a_write_for_the_wrong_group_is_not_appended() {
    let driver = NumberedDriver::new_elected(NodeId(1), vec![numbered_group(7, 1, &[], 3)])
        .expect("numbered primary elects");
    let wrong_handle: RaftHandle<
        u64,
        (String, String),
        String,
        Option<String>,
        Option<String>,
        NumberedDriver,
    > = RaftHandle::new(8, driver.clone());

    let error = block_on(wrong_handle.write(("alpha".to_owned(), "one".to_owned())))
        .expect_err("the driver does not own group 8");

    assert_eq!(error.kind(), WriteErrorKind::WrongGroup);
    assert_eq!(
        error.fate(),
        WriteFate::NotAppended,
        "a command the driver never looked at leaves its request identity unused"
    );

    let read = block_on(wrong_handle.read("alpha".to_owned(), ReadConsistency::Linearizable))
        .expect_err("the driver does not own group 8");
    assert_eq!(read.kind(), ReadErrorKind::WrongGroup);
}

/// One failure, one category, two fates. The old code cloned a single error
/// across every entry of the batch, which told a client that an appended entry
/// had been refused.
#[test]
fn a_batch_failure_reports_a_per_entry_fate() {
    let driver = scripted_write_driver(ScriptedWriteMode::AppendThenMissingNode);

    let outcomes = block_on(driver.write_batch(
        (),
        vec![
            WriteBatchEntry::new(("alpha".to_owned(), "one".to_owned())),
            WriteBatchEntry::new(("beta".to_owned(), "two".to_owned())),
        ],
    ));

    let fates = outcomes
        .iter()
        .map(|outcome| {
            outcome
                .as_ref()
                .err()
                .map(WriteError::fate)
                .expect("every entry of this batch fails")
        })
        .collect::<Vec<_>>();

    assert!(
        fates.iter().all(|fate| *fate == WriteFate::Unresolved),
        "both entries appended before routing failed, so neither is refused: {fates:?}"
    );
}

#[test]
fn in_memory_driver_maps_no_lifecycle_proposal_output_to_unknown_outcome() {
    let driver = scripted_write_driver(ScriptedWriteMode::PreAppendNoLifecycleMessage);
    let handle = driver.handle();

    let error = block_on(handle.write(("alpha".to_owned(), "one".to_owned())))
        .expect_err("the runtime produced no proposal lifecycle output");

    assert!(
        matches!(
            error,
            WriteError::UnknownOutcome {
                local_proposal_id: LocalProposalId(1),
                client_request_id: None,
                reason: UnknownOutcomeReason::RuntimeDroppedProposal,
            }
        ),
        "got {error:?}"
    );
    assert_eq!(error.kind(), WriteErrorKind::UnknownOutcome);
    assert_eq!(
        handle
            .metrics()
            .expect("metrics")
            .current()
            .pending_proposals,
        0,
        "malformed no-lifecycle output is cleaned up before returning"
    );
}

/// The counterpart to a constructor that takes its groups by value: nothing
/// returned them before, so a driver was where a group went to be unreachable.
#[test]
fn release_groups_returns_every_group_the_driver_adopted() {
    let driver = KvDriver::new_elected(NodeId(1), groups()).expect("primary elects");
    block_on(
        driver
            .handle()
            .write(("alpha".to_owned(), "one".to_owned())),
    )
    .expect("the write commits and applies");

    let released = driver
        .release_groups()
        .expect("the driver holds its groups");

    assert_eq!(
        released.keys().copied().collect::<Vec<_>>(),
        vec![NodeId(1), NodeId(2), NodeId(3)]
    );
    let primary = &released[&NodeId(1)];
    assert_eq!(primary.state_machine().values["alpha"], "one");
    assert_eq!(primary.metrics().pending_proposals, 0);
}

#[test]
fn a_released_in_memory_driver_refuses_every_operation() {
    let driver = KvDriver::new_elected(NodeId(1), groups()).expect("primary elects");
    let handle = driver.handle();
    let _ = driver
        .release_groups()
        .expect("the driver holds its groups");

    assert!(matches!(
        driver.release_groups().map(|_| ()),
        Err(ManagedDriverError::ShuttingDown)
    ));
    assert!(matches!(
        driver.tick_primary(),
        Err(ManagedDriverError::ShuttingDown)
    ));

    let write = block_on(handle.write(("beta".to_owned(), "two".to_owned())))
        .expect_err("a released driver serves no writes");
    assert!(matches!(write, WriteError::ShuttingDown), "got {write:?}");
    assert_eq!(
        write.fate(),
        WriteFate::NotAppended,
        "a refused write never reached the log"
    );
}
