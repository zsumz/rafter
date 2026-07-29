#![allow(clippy::wildcard_imports)]

mod support;

use std::collections::BTreeSet;

use rafter_invariant_test::oracle_assert;
use support::*;

#[test]
fn read_barrier_returns_proof_when_local_apply_is_fresh() {
    let read_id = ReadId(50);
    let runtime = ScriptedRuntime::with_step_outputs([vec![RaftOutput::ReadIndexGranted {
        read_id,
        read_index: LogIndex(4),
    }]]);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine {
            applied_index: LogIndex(5),
            ..RecordingStateMachine::default()
        },
        runtime,
    );

    let outcome = group
        .begin_read_barrier_outcome(read_request(read_id, Some(LogIndex(5))))
        .expect("read barrier starts");
    assert!(matches!(
        outcome,
        ReadProofOutcome::Granted {
            proof: ReadProof {
                group_id: 7,
                issued_by: NodeId(1),
                term: Term(2),
                read_index: LogIndex(4),
                required_applied_index: LogIndex(5),
                local_applied_index: LogIndex(5),
            }
        }
    ));
    assert_eq!(group.metrics().pending_read_barriers, 0);
}

#[test]
fn begin_read_barrier_preserves_coemitted_report_streams() {
    let read_id = ReadId(51);
    let snapshot = test_snapshot(13);
    let staged = staged_snapshot_chunk(&snapshot);
    let updated_membership = membership(&[1, 2], &[]);
    let mut runtime = ScriptedRuntime::with_step_outputs([vec![
        RaftOutput::ReadIndexGranted {
            read_id,
            read_index: LogIndex(4),
        },
        RaftOutput::StageSnapshotChunk {
            chunk: staged.clone(),
        },
        RaftOutput::LeadershipTransferRejected {
            target: NodeId(3),
            reason: LeadershipTransferRejection::TargetIsSelf,
        },
    ]]);
    runtime.commit_index = LogIndex(5);
    runtime
        .step_memberships
        .push_back((updated_membership.clone(), updated_membership.clone()));
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine {
            applied_index: LogIndex(5),
            ..RecordingStateMachine::default()
        },
        runtime,
    );

    let full = group
        .begin_read_barrier(read_request(read_id, None))
        .expect("read barrier starts with full report");

    assert!(matches!(
        full.outcome,
        ReadProofOutcome::Granted {
            proof: ReadProof {
                group_id: 7,
                read_index: LogIndex(4),
                required_applied_index: LogIndex(4),
                local_applied_index: LogIndex(5),
                ..
            }
        }
    ));
    assert_eq!(full.report.read_events.len(), 1);
    assert_eq!(
        full.report.snapshot_events,
        vec![SnapshotEvent::StageChunk {
            group_id: 7,
            chunk: staged,
        }]
    );
    // The scripted step moves both facts at once, so both are reported and the
    // effective one comes first; a read-barrier step carries membership events
    // like any other, which is the clause under test here. The committed one is
    // an endpoint observation because this scripted runtime emits no
    // `ConfigurationCommitted` output for the move, so there is no configuration
    // entry to name and the comparison at the commit index is what reports it.
    assert_eq!(
        full.report.membership_events,
        vec![
            MembershipEvent::EffectiveChanged {
                group_id: 7,
                index: LogIndex::ZERO,
                term: Term(2),
                membership: updated_membership.clone(),
            },
            MembershipEvent::CommittedEndpoint {
                group_id: 7,
                index: LogIndex(5),
                term: Term(1),
                membership: updated_membership,
            }
        ]
    );
    assert_eq!(
        full.report.leadership_transfer_events,
        vec![LeadershipTransferEvent::Rejected {
            target: NodeId(3),
            reason: LeadershipTransferRejection::TargetIsSelf,
            leader_hint: Some(NodeId(1)),
        }]
    );
}

#[test]
fn local_read_helper_returns_state_machine_result_without_raft_step() {
    let read_id = ReadId(55);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine {
            applied_index: LogIndex(5),
            ..RecordingStateMachine::default()
        },
        ScriptedRuntime::with_step_outputs([]),
    );

    let outcome = group
        .read(read_helper_request(read_id, ReadConsistency::Local, None))
        .expect("local read succeeds")
        .outcome;

    assert_eq!(
        outcome,
        ReadOutcome::Ready {
            result: Some(b"query".to_vec()),
            proof: None,
        }
    );
    assert!(group.runtime().step_inputs.is_empty());
    assert_eq!(group.metrics().pending_read_barriers, 0);
    assert_eq!(group.read_id_watermark(), None);
}

#[test]
fn local_read_helper_reports_requested_freshness_gap() {
    let read_id = ReadId(56);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine {
            applied_index: LogIndex(2),
            ..RecordingStateMachine::default()
        },
        ScriptedRuntime::with_step_outputs([]),
    );

    let outcome = group
        .read(read_helper_request(
            read_id,
            ReadConsistency::Local,
            Some(LogIndex(5)),
        ))
        .expect("local read reports freshness")
        .outcome;

    assert_eq!(
        outcome,
        ReadOutcome::LocalFreshnessUnavailable {
            required_applied_index: LogIndex(5),
            local_applied_index: LogIndex(2),
        }
    );
    assert_eq!(group.read_id_watermark(), None);

    let barrier = group
        .begin_read_barrier_outcome(read_request(read_id, None))
        .expect("local freshness gap does not consume the read id");
    assert_eq!(
        barrier,
        ReadProofOutcome::Pending {
            read_id,
            peer_messages: Vec::new(),
        }
    );
    assert_eq!(group.read_id_watermark(), Some(read_id));
}

#[test]
fn lease_read_helper_is_explicitly_unsupported_without_lease_support() {
    let read_id = ReadId(57);
    let mut group = scripted_group(RecordingStateMachine::default());

    let error = group
        .read(read_helper_request(
            read_id,
            ReadConsistency::LeaseRead,
            None,
        ))
        .expect_err("lease read is not silently treated as safe");

    assert!(matches!(
        error,
        GroupError::UnsupportedReadConsistency {
            consistency: ReadConsistency::LeaseRead,
        }
    ));
}

#[test]
fn linearizable_read_helper_returns_result_when_barrier_grants() {
    let read_id = ReadId(58);
    let runtime = ScriptedRuntime::with_step_outputs([vec![RaftOutput::ReadIndexGranted {
        read_id,
        read_index: LogIndex(4),
    }]]);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine {
            applied_index: LogIndex(5),
            ..RecordingStateMachine::default()
        },
        runtime,
    );

    let outcome = group
        .read(read_helper_request(
            read_id,
            ReadConsistency::Linearizable,
            Some(LogIndex(5)),
        ))
        .expect("linearizable read succeeds")
        .outcome;

    oracle_assert!(matches!(
        outcome,
        ReadOutcome::Ready {
            result: Some(ref result),
            proof: Some(ReadProof {
                group_id: 7,
                issued_by: NodeId(1),
                term: Term(2),
                read_index: LogIndex(4),
                required_applied_index: LogIndex(5),
                local_applied_index: LogIndex(5),
            }),
        } if result == b"query"
    ));
    assert_eq!(group.metrics().pending_read_barriers, 0);
}

#[test]
fn pending_linearizable_read_helper_completes_after_normal_progress() {
    let read_id = ReadId(59);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine {
            applied_index: LogIndex(4),
            ..RecordingStateMachine::default()
        },
        ScriptedRuntime::with_step_outputs([]),
    );

    let first = group
        .read(read_helper_request(
            read_id,
            ReadConsistency::Linearizable,
            None,
        ))
        .expect("linearizable read starts")
        .outcome;
    assert_eq!(
        first,
        ReadOutcome::Pending {
            read_id,
            peer_messages: Vec::new(),
        }
    );
    assert_eq!(group.metrics().pending_read_barriers, 1);
    assert_eq!(group.runtime().step_inputs.len(), 1);

    let retry = group
        .read(read_helper_request(
            read_id,
            ReadConsistency::Linearizable,
            None,
        ))
        .expect("pending linearizable read can be retried")
        .outcome;
    assert_eq!(
        retry,
        ReadOutcome::Pending {
            read_id,
            peer_messages: Vec::new(),
        }
    );
    assert_eq!(
        group.runtime().step_inputs.len(),
        1,
        "retry must not start another read-index round"
    );

    let report = group
        .apply_raft_outputs(vec![RaftOutput::ReadIndexGranted {
            read_id,
            read_index: LogIndex(4),
        }])
        .expect("normal progress grants pending read");
    assert!(report.read_events.iter().any(|event| matches!(
        event,
        ReadEvent::Granted {
            read_id: event_read_id,
            ..
        } if *event_read_id == read_id
    )));
    assert_eq!(group.metrics().pending_read_barriers, 0);

    let completed = group
        .read(read_helper_request(
            read_id,
            ReadConsistency::Linearizable,
            None,
        ))
        .expect("completed proof drives state-machine read")
        .outcome;
    assert!(matches!(
        completed,
        ReadOutcome::Ready {
            result: Some(ref result),
            proof: Some(ReadProof {
                read_index: LogIndex(4),
                required_applied_index: LogIndex(4),
                local_applied_index: LogIndex(4),
                ..
            }),
        } if result == b"query"
    ));
    assert_read_metrics(&group, 0, 0, 0, 0);
}

#[test]
fn duplicate_begin_read_barrier_rejects_without_overwriting_pending_state() {
    let read_id = ReadId(61);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine {
            applied_index: LogIndex(4),
            ..RecordingStateMachine::default()
        },
        ScriptedRuntime::with_step_outputs([]),
    );

    let first = group
        .begin_read_barrier_outcome(read_request(read_id, None))
        .expect("first read barrier starts");
    assert_eq!(
        first,
        ReadProofOutcome::Pending {
            read_id,
            peer_messages: Vec::new(),
        }
    );

    let error = group
        .begin_read_barrier_outcome(read_request(read_id, Some(LogIndex(9))))
        .expect_err("duplicate active read id is rejected");

    assert!(matches!(
        error,
        GroupError::DuplicateReadId {
            read_id: duplicate
        } if duplicate == read_id
    ));
    assert_read_metrics(&group, 1, 0, 0, 1);
    assert_eq!(
        group.runtime().step_inputs.len(),
        1,
        "duplicate barrier must not start another read-index round"
    );
}

#[test]
fn begin_read_barrier_rejects_lower_read_id_after_higher_seen() {
    let higher_id = ReadId(71);
    let lower_id = ReadId(70);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine {
            applied_index: LogIndex(4),
            ..RecordingStateMachine::default()
        },
        ScriptedRuntime::with_step_outputs([]),
    );

    let first = group
        .begin_read_barrier_outcome(read_request(higher_id, None))
        .expect("higher read barrier starts");
    assert_eq!(
        first,
        ReadProofOutcome::Pending {
            read_id: higher_id,
            peer_messages: Vec::new(),
        }
    );

    let error = group
        .begin_read_barrier_outcome(read_request(lower_id, None))
        .expect_err("lower read id is rejected after higher watermark");
    assert_non_monotonic_read_id(&error, lower_id, higher_id);
    assert_eq!(
        group.runtime().step_inputs.len(),
        1,
        "non-monotonic read id must not start another read-index round"
    );
}

#[test]
fn wrong_group_read_barrier_does_not_consume_read_id() {
    let read_id = ReadId(72);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine::default(),
        ScriptedRuntime::with_step_outputs([]),
    );
    let error = group
        .begin_read_barrier_outcome(ReadBarrierRequest {
            group_id: 8,
            read_id,
            min_applied_index: None,
            context: Vec::new(),
        })
        .expect_err("wrong group is rejected before submission");

    assert!(matches!(error, GroupError::WrongGroup));
    assert_eq!(group.read_id_watermark(), None);
    assert!(group.runtime().step_inputs.is_empty());

    group
        .begin_read_barrier_outcome(read_request(read_id, None))
        .expect("same read id is still available after wrong-group rejection");
    assert_eq!(group.read_id_watermark(), Some(read_id));
}

#[test]
fn poisoned_group_read_barrier_does_not_consume_read_id() {
    let read_id = ReadId(73);
    let mut group = scripted_group(RecordingStateMachine {
        apply_mode: ApplyMode::Fail,
        ..RecordingStateMachine::default()
    });
    let _ = group
        .apply_raft_outputs(vec![apply_output(2, b"bad", Some(LocalProposalId(73)))])
        .expect_err("apply failure poisons group");

    let error = group
        .begin_read_barrier_outcome(read_request(read_id, None))
        .expect_err("poisoned group rejects read barriers before submission");

    assert!(matches!(error, GroupError::Poisoned { .. }));
    assert_eq!(group.read_id_watermark(), None);
}

#[test]
fn runtime_step_error_consumes_read_id() {
    let read_id = ReadId(74);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine::default(),
        ScriptedRuntime::with_step_errors([TestRuntimeError::Forced]),
    );

    let error = group
        .begin_read_barrier_outcome(read_request(read_id, None))
        .expect_err("runtime error is returned");

    assert!(matches!(error, GroupError::Runtime(_)));
    assert_eq!(group.read_id_watermark(), Some(read_id));
    assert_eq!(group.metrics().pending_read_barriers, 0);
    assert_eq!(group.runtime().step_inputs.len(), 1);

    let reuse = group
        .begin_read_barrier_outcome(read_request(read_id, None))
        .expect_err("submitted read id is consumed by runtime error");
    assert_non_monotonic_read_id(&reuse, read_id, read_id);
}

#[test]
fn read_index_rejected_consumes_read_id() {
    let read_id = ReadId(75);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine::default(),
        ScriptedRuntime::with_step_outputs([vec![RaftOutput::ReadIndexRejected {
            read_id,
            reason: ReadIndexRejection::NotLeader {
                role: Role::Follower,
                term: Term(3),
            },
        }]]),
    );

    let outcome = group
        .begin_read_barrier_outcome(read_request(read_id, None))
        .expect("read rejection is reported");
    assert!(matches!(outcome, ReadProofOutcome::Rejected { .. }));
    assert_eq!(group.read_id_watermark(), Some(read_id));
    assert_eq!(group.metrics().pending_read_barriers, 0);

    let reuse = group
        .begin_read_barrier_outcome(read_request(read_id, None))
        .expect_err("rejected read id remains consumed");
    assert_non_monotonic_read_id(&reuse, read_id, read_id);
}

#[test]
fn read_index_canceled_consumes_read_id() {
    let read_id = ReadId(76);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine::default(),
        ScriptedRuntime::with_step_outputs([vec![RaftOutput::ReadIndexCanceled {
            read_id,
            reason: ReadIndexCancelReason::LeadershipLost,
        }]]),
    );

    let outcome = group
        .begin_read_barrier_outcome(read_request(read_id, None))
        .expect("read cancellation is reported");
    assert!(matches!(outcome, ReadProofOutcome::Canceled { .. }));
    assert_eq!(group.read_id_watermark(), Some(read_id));
    assert_eq!(group.metrics().pending_read_barriers, 0);

    let reuse = group
        .begin_read_barrier_outcome(read_request(read_id, None))
        .expect_err("canceled read id remains consumed");
    assert_non_monotonic_read_id(&reuse, read_id, read_id);
}

/// The effect `ReadOutcome::Pending` structurally cannot carry.
///
/// A read-index broadcast reaches snapshot streaming for any follower behind the
/// snapshot boundary, and an embedder with its own runtime must deliver that
/// directive. The outcome value has nowhere to put it.
#[test]
fn read_report_carries_snapshot_chunk_directives_emitted_by_the_barrier_step() {
    let read_id = ReadId(80);
    let snapshot = test_snapshot(11);
    let send = snapshot_chunk_send(&snapshot);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine::default(),
        ScriptedRuntime::with_step_outputs([vec![RaftOutput::SendSnapshotChunk {
            to: NodeId(2),
            chunk: send.clone(),
        }]]),
    );

    let read = group
        .read(read_helper_request(
            read_id,
            ReadConsistency::Linearizable,
            None,
        ))
        .expect("linearizable read starts");

    assert!(matches!(read.outcome, ReadOutcome::Pending { .. }));
    assert_eq!(
        read.report.snapshot_events,
        vec![SnapshotEvent::SendChunk {
            group_id: 7,
            to: NodeId(2),
            chunk: send,
        }]
    );
}

/// Every step resolves every pending barrier whose read index is satisfied, so
/// one barrier's proof can be emitted inside another barrier's read. The proof
/// is the only copy: resolution removes the barrier from the pending table.
#[test]
fn read_report_carries_another_barriers_granted_event() {
    let stalled_id = ReadId(81);
    let read_id = ReadId(82);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine {
            applied_index: LogIndex(2),
            ..RecordingStateMachine::default()
        },
        ScriptedRuntime::with_step_outputs([
            vec![RaftOutput::ReadIndexGranted {
                read_id: stalled_id,
                read_index: LogIndex(4),
            }],
            Vec::new(),
        ]),
    );

    // Barrier A is granted a read index its state machine has not reached.
    let stalled = group
        .begin_read_barrier_outcome(read_request(stalled_id, None))
        .expect("barrier A starts");
    assert!(matches!(
        stalled,
        ReadProofOutcome::FreshnessUnavailable { .. }
    ));

    // The documented maintenance hook advances the applied floor behind the
    // group, which is what makes A resolvable during an unrelated step.
    group.state_machine_mut().applied_index = LogIndex(4);

    let read = group
        .read(read_helper_request(
            read_id,
            ReadConsistency::Linearizable,
            None,
        ))
        .expect("barrier B starts");

    assert!(read.report.read_events.iter().any(|event| matches!(
        event,
        ReadEvent::Granted {
            read_id: granted_id,
            proof,
        } if *granted_id == stalled_id && proof.read_index == LogIndex(4)
    )));
    assert_eq!(group.metrics().pending_read_barriers, 1);
}

/// The documented footgun, pinned: the outcome-only form destroys the other
/// barrier's proof, and no later step re-emits it because resolution already
/// removed that barrier from the pending table.
#[test]
fn read_outcome_discards_co_emitted_read_events() {
    let stalled_id = ReadId(83);
    let read_id = ReadId(84);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine {
            applied_index: LogIndex(2),
            ..RecordingStateMachine::default()
        },
        ScriptedRuntime::with_step_outputs([
            vec![RaftOutput::ReadIndexGranted {
                read_id: stalled_id,
                read_index: LogIndex(4),
            }],
            Vec::new(),
            Vec::new(),
        ]),
    );
    let _ = group
        .begin_read_barrier_outcome(read_request(stalled_id, None))
        .expect("barrier A starts");
    group.state_machine_mut().applied_index = LogIndex(4);

    let outcome = group
        .read_outcome(read_helper_request(
            read_id,
            ReadConsistency::Linearizable,
            None,
        ))
        .expect("barrier B starts");

    assert!(matches!(outcome, ReadOutcome::Pending { .. }));
    // A is gone from the pending table and its proof was never handed out.
    assert_eq!(group.metrics().pending_read_barriers, 1);
    let later = group
        .step(GroupInput::Tick)
        .expect("driving the group further cannot recover the proof");
    assert!(later.read_events.is_empty());
    assert!(!group.cancel_read(stalled_id));
}

/// A local read never steps the runtime, so its report is empty for this group
/// — but it is still a report, so a caller routes it unconditionally.
#[test]
fn read_report_is_empty_for_local_reads() {
    let read_id = ReadId(85);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine {
            applied_index: LogIndex(5),
            ..RecordingStateMachine::default()
        },
        ScriptedRuntime::with_step_outputs([]),
    );

    let read = group
        .read(read_helper_request(read_id, ReadConsistency::Local, None))
        .expect("local read succeeds");

    assert_empty_report(&read.report);
    assert!(group.runtime().step_inputs.is_empty());
}

/// A retry that consumes an already completed proof does not step either, so
/// the outcome-only form is lossless for exactly this case.
#[test]
fn read_report_is_empty_when_consuming_a_completed_proof() {
    let read_id = ReadId(86);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine {
            applied_index: LogIndex(2),
            ..RecordingStateMachine::default()
        },
        ScriptedRuntime::with_step_outputs([
            Vec::new(),
            vec![RaftOutput::ReadIndexGranted {
                read_id,
                read_index: LogIndex(4),
            }],
        ]),
    );
    let first = group
        .read(read_helper_request(
            read_id,
            ReadConsistency::Linearizable,
            None,
        ))
        .expect("linearizable read starts");
    assert!(matches!(first.outcome, ReadOutcome::Pending { .. }));
    group.state_machine_mut().applied_index = LogIndex(4);
    let granted = group
        .step(GroupInput::Tick)
        .expect("the barrier completes on a later step");
    assert!(granted
        .read_events
        .iter()
        .any(|event| matches!(event, ReadEvent::Granted { .. })));
    let steps_before_retry = group.runtime().step_inputs.len();

    let retry = group
        .read(read_helper_request(
            read_id,
            ReadConsistency::Linearizable,
            None,
        ))
        .expect("the retry consumes the completed proof");

    assert!(matches!(retry.outcome, ReadOutcome::Ready { .. }));
    assert_empty_report(&retry.report);
    assert_eq!(group.runtime().step_inputs.len(), steps_before_retry);
}

/// Negative: a misrouted request is refused before anything runs, so there is
/// no report to route and no read state to clean up.
#[test]
fn read_rejects_a_wrong_group_request_without_a_report() {
    let mut group = scripted_group(RecordingStateMachine::default());

    let error = group
        .read(ReadRequest::Linearizable {
            group_id: 9,
            read_id: ReadId(87),
            query: b"query".to_vec(),
            min_applied_index: None,
            context: Vec::new(),
        })
        .expect_err("a request for another group is refused");

    assert!(matches!(error, GroupError::WrongGroup));
    assert!(group.runtime().step_inputs.is_empty());
    assert_eq!(group.read_id_watermark(), None);
}

/// Negative: poison is checked before anything runs, so a poisoned group
/// produces no report on this path either.
#[test]
fn read_rejects_a_poisoned_group_without_a_report() {
    let mut group = scripted_group(RecordingStateMachine {
        apply_mode: ApplyMode::Fail,
        ..RecordingStateMachine::default()
    });
    let _ = group
        .apply_raft_outputs(vec![apply_output(2, b"bad", Some(LocalProposalId(88)))])
        .expect_err("apply failure poisons the group");
    let steps_before = group.runtime().step_inputs.len();

    let error = group
        .read(read_helper_request(
            ReadId(88),
            ReadConsistency::Linearizable,
            None,
        ))
        .expect_err("a poisoned group refuses reads");

    assert!(matches!(error, GroupError::Poisoned { .. }));
    assert_eq!(group.runtime().step_inputs.len(), steps_before);
}

/// Negative: a terminal read event clears local waiter state, so retrying after
/// one is a non-monotonic ID error rather than a second statement of the
/// rejection. The report is where a caller learns to stop.
#[test]
fn read_retry_after_a_terminal_read_event_is_non_monotonic() {
    let read_id = ReadId(89);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine::default(),
        ScriptedRuntime::with_step_outputs([vec![RaftOutput::ReadIndexRejected {
            read_id,
            reason: ReadIndexRejection::NotLeader {
                role: Role::Follower,
                term: Term(2),
            },
        }]]),
    );

    let read = group
        .read(read_helper_request(
            read_id,
            ReadConsistency::Linearizable,
            None,
        ))
        .expect("the rejection is reported as an outcome");
    assert!(matches!(read.outcome, ReadOutcome::Rejected { .. }));
    assert!(read
        .report
        .read_events
        .iter()
        .any(|event| matches!(event, ReadEvent::Rejected { .. })));

    let retry = group
        .read(read_helper_request(
            read_id,
            ReadConsistency::Linearizable,
            None,
        ))
        .expect_err("a rejected read id stays consumed");

    assert_non_monotonic_read_id(&retry, read_id, read_id);
}

/// The headline defect. A new leader's first entry in its term is a `Noop`, so
/// the barrier grants at an index the state machine is never told about. Before
/// the floor was introduced this read stalled forever on a read-only tail.
#[test]
fn read_barrier_grants_when_the_read_index_is_a_non_application_entry() {
    let read_id = ReadId(90);
    let mut runtime = ScriptedRuntime::with_step_outputs([vec![RaftOutput::ReadIndexGranted {
        read_id,
        read_index: LogIndex(6),
    }]]);
    runtime.commit_index = LogIndex(6);
    runtime.application_entries = Some([LogIndex(3), LogIndex(4)].into_iter().collect());
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine {
            applied_index: LogIndex(4),
            ..RecordingStateMachine::default()
        },
        runtime,
    );

    let outcome = group
        .read(read_helper_request(
            read_id,
            ReadConsistency::Linearizable,
            None,
        ))
        .expect("linearizable read succeeds")
        .outcome;

    oracle_assert!(
        matches!(
            outcome,
            ReadOutcome::Ready {
                result: Some(ref result),
                proof: Some(ReadProof {
                    read_index: LogIndex(6),
                    required_applied_index: LogIndex(4),
                    local_applied_index: LogIndex(4),
                    ..
                }),
            } if result == b"query"
        ),
        "a barrier granted at a non-application entry must require only the \
         application floor below it, got {outcome:?}"
    );
    assert_eq!(group.metrics().pending_read_barriers, 0);
}

/// The extreme case: a cluster whose only entry is its first leader's `Noop`.
/// Its first-ever linearizable read must answer without anyone writing first.
#[test]
fn read_barrier_grants_on_a_cluster_that_has_committed_no_application_entry() {
    let read_id = ReadId(91);
    let mut runtime = ScriptedRuntime::with_step_outputs([vec![RaftOutput::ReadIndexGranted {
        read_id,
        read_index: LogIndex(1),
    }]]);
    runtime.commit_index = LogIndex(1);
    runtime.application_entries = Some(BTreeSet::new());
    let mut group = scripted_group_with_runtime(RecordingStateMachine::default(), runtime);

    let outcome = group
        .read(read_helper_request(
            read_id,
            ReadConsistency::Linearizable,
            None,
        ))
        .expect("the first read of a cluster's life succeeds");

    oracle_assert!(
        matches!(
            outcome.outcome,
            ReadOutcome::Ready {
                proof: Some(ReadProof {
                    read_index: LogIndex(1),
                    required_applied_index: LogIndex::ZERO,
                    local_applied_index: LogIndex::ZERO,
                    ..
                }),
                ..
            }
        ),
        "a cluster with no committed application entry requires nothing of its \
         state machine, got {:?}",
        outcome.outcome
    );
}

/// The mixed-log case, and the direct refutation of an uncapped floor. Entry 7
/// commits while the barrier's round is in flight; it is not ordered before
/// this read and waiting for it would be a defect, not caution.
#[test]
fn read_barrier_does_not_require_an_application_entry_above_the_read_index() {
    let read_id = ReadId(92);
    let mut runtime = ScriptedRuntime::with_step_outputs([vec![RaftOutput::ReadIndexGranted {
        read_id,
        read_index: LogIndex(6),
    }]]);
    runtime.commit_index = LogIndex(7);
    runtime.application_entries = Some([LogIndex(4), LogIndex(7)].into_iter().collect());
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine {
            applied_index: LogIndex(4),
            ..RecordingStateMachine::default()
        },
        runtime,
    );

    let outcome = group
        .read(read_helper_request(
            read_id,
            ReadConsistency::Linearizable,
            None,
        ))
        .expect("linearizable read succeeds")
        .outcome;

    oracle_assert!(
        matches!(
            outcome,
            ReadOutcome::Ready {
                proof: Some(ReadProof {
                    read_index: LogIndex(6),
                    required_applied_index: LogIndex(4),
                    ..
                }),
                ..
            }
        ),
        "an application entry above the read index is not ordered before the \
         read and must not be required, got {outcome:?}"
    );
}

/// The floor is resolved once, at grant, and stored. A caller polling toward
/// `FreshnessUnavailable` therefore sees a stable target that a later commit or
/// compaction cannot move.
#[test]
fn read_barrier_floor_is_fixed_at_grant() {
    let read_id = ReadId(93);
    let mut runtime = ScriptedRuntime::with_step_outputs([vec![RaftOutput::ReadIndexGranted {
        read_id,
        read_index: LogIndex(6),
    }]]);
    runtime.commit_index = LogIndex(6);
    runtime.application_entries = Some([LogIndex(3)].into_iter().collect());
    // The barrier step consumes the first shape unchanged; the tick after it
    // commits an application entry at 5 and compacts, which is exactly the
    // reshape that would move a re-derived floor from 3 to 5.
    //
    // The boundary stays at this state machine's own applied index. Compacting
    // above it would script a composition no embedder can produce —
    // `build_snapshot` requires the state machine's applied index as its
    // boundary — and the group now refuses to run on one, so the fixture would
    // be testing the refusal rather than the barrier.
    runtime.step_log_shapes = [
        ScriptedLogShape {
            application_entries: Some([LogIndex(3)].into_iter().collect()),
            commit_index: LogIndex(6),
            snapshot_index: LogIndex::ZERO,
        },
        ScriptedLogShape {
            application_entries: Some([LogIndex(3), LogIndex(5)].into_iter().collect()),
            commit_index: LogIndex(8),
            snapshot_index: LogIndex(2),
        },
    ]
    .into_iter()
    .collect();
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine {
            applied_index: LogIndex(2),
            ..RecordingStateMachine::default()
        },
        runtime,
    );

    let outcome = group
        .begin_read_barrier_outcome(read_request(read_id, None))
        .expect("barrier starts and stalls");
    assert_eq!(
        outcome,
        ReadProofOutcome::FreshnessUnavailable {
            read_id,
            required_applied_index: LogIndex(3),
            local_applied_index: LogIndex(2),
        }
    );

    for step in 0..2 {
        let report = group
            .step(GroupInput::Tick)
            .expect("driving the group re-examines the stalled barrier");
        oracle_assert!(
            report.read_events
                == vec![ReadEvent::FreshnessUnavailable {
                    read_id,
                    required_applied_index: LogIndex(3),
                    local_applied_index: LogIndex(2),
                }],
            "step {step} must report the floor resolved at grant, got {:?}",
            report.read_events
        );
    }
    assert_eq!(
        group
            .runtime()
            .committed_application_index_through(LogIndex(6)),
        LogIndex(5),
        "a re-derived floor would have moved, which is what the stored one avoids"
    );
}

/// A caller-supplied floor is honored verbatim: not capped at the read index,
/// not lowered to an application entry, not silently repaired.
#[test]
fn read_barrier_honors_a_caller_supplied_floor_verbatim() {
    let read_id = ReadId(94);
    let mut runtime = ScriptedRuntime::with_step_outputs([vec![RaftOutput::ReadIndexGranted {
        read_id,
        read_index: LogIndex(6),
    }]]);
    runtime.commit_index = LogIndex(6);
    runtime.application_entries = Some([LogIndex(3)].into_iter().collect());
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine {
            applied_index: LogIndex(9),
            ..RecordingStateMachine::default()
        },
        runtime,
    );

    let outcome = group
        .begin_read_barrier_outcome(read_request(read_id, Some(LogIndex(9))))
        .expect("barrier starts");

    oracle_assert!(
        matches!(
            outcome,
            ReadProofOutcome::Granted {
                proof: ReadProof {
                    read_index: LogIndex(6),
                    required_applied_index: LogIndex(9),
                    local_applied_index: LogIndex(9),
                    ..
                }
            }
        ),
        "a caller floor above the read index must dominate and must not be \
         capped, got {outcome:?}"
    );
}

/// Negative: the stale-read attempt the correctness argument must survive.
///
/// This test tries to construct a stale read and must fail to. Application
/// entries commit at 3 and 5, the leader's `Noop` at 6 is the read index, and
/// the state machine has applied only through 3 — so entry 5 is an
/// acknowledged write inside the cut that the state machine has not
/// incorporated. Serving here would answer from state missing that write and
/// break both `RD-04` and `RD-06`.
///
/// The two ways to get there are both attacked. Computing the floor as
/// anything but the *highest* application entry in the cut — the lowest, or the
/// snapshot boundary, or the state machine's own cursor — yields 3 or less and
/// serves immediately; the assertion below rejects every one of those. Treating
/// the `Noop` at 6 as application-visible would raise the floor to an index no
/// state machine can reach, which the sibling tests reject from the other side.
#[test]
fn read_barrier_does_not_grant_while_an_application_entry_below_the_read_index_is_unapplied() {
    let read_id = ReadId(95);
    let mut runtime = ScriptedRuntime::with_step_outputs([
        vec![RaftOutput::ReadIndexGranted {
            read_id,
            read_index: LogIndex(6),
        }],
        Vec::new(),
    ]);
    runtime.commit_index = LogIndex(6);
    runtime.application_entries = Some([LogIndex(3), LogIndex(5)].into_iter().collect());
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine {
            applied_index: LogIndex(3),
            applied: vec![b"three".to_vec()],
            ..RecordingStateMachine::default()
        },
        runtime,
    );

    let stalled = group
        .read(state_read_request(read_id, None))
        .expect("the barrier starts")
        .outcome;

    oracle_assert!(
        matches!(
            stalled,
            ReadOutcome::LinearizableFreshnessUnavailable {
                required_applied_index: LogIndex(5),
                local_applied_index: LogIndex(3),
                ..
            }
        ),
        "an unapplied application entry inside the cut must hold the barrier, \
         got {stalled:?}"
    );

    // Entry 5's effect reaches the state machine only now.
    let _ = group
        .apply_raft_outputs(vec![apply_output(5, b"five", None)])
        .expect("entry 5 applies");

    let served = group
        .read(state_read_request(read_id, None))
        .expect("the barrier grants once entry 5 is applied")
        .outcome;

    oracle_assert!(
        matches!(
            served,
            ReadOutcome::Ready {
                result: Some(ref result),
                proof: Some(ReadProof {
                    read_index: LogIndex(6),
                    required_applied_index: LogIndex(5),
                    local_applied_index: LogIndex(5),
                    ..
                }),
            } if result == b"five"
        ),
        "the answer must carry entry 5's effect, not the state that predates \
         it, got {served:?}"
    );
}

/// Negative: no derivation may reintroduce a floor above the cut, which is the
/// failure an uncapped `committed_application_index()` would produce.
#[test]
fn read_barrier_floor_never_exceeds_the_read_index() {
    let entry_sets = [
        None,
        Some(BTreeSet::new()),
        Some([LogIndex(1)].into_iter().collect::<BTreeSet<_>>()),
        Some([LogIndex(3), LogIndex(5)].into_iter().collect()),
        Some(
            [LogIndex(2), LogIndex(4), LogIndex(6), LogIndex(7)]
                .into_iter()
                .collect(),
        ),
    ];
    let mut read_id = 100_u64;
    for entries in entry_sets {
        for snapshot in [0_u64, 2, 9] {
            for read_index in [0_u64, 1, 3, 4, 6, 7] {
                read_id += 1;
                let read_id = ReadId(read_id);
                let mut runtime =
                    ScriptedRuntime::with_step_outputs([vec![RaftOutput::ReadIndexGranted {
                        read_id,
                        read_index: LogIndex(read_index),
                    }]]);
                runtime.commit_index = LogIndex(9);
                runtime.snapshot_index = LogIndex(snapshot);
                runtime.application_entries = entries.clone();
                oracle_assert!(
                    runtime.committed_application_index_through(LogIndex(read_index))
                        <= LogIndex(read_index),
                    "derivation exceeded its bound for entries {entries:?} \
                     snapshot {snapshot} bound {read_index}"
                );

                let mut group = scripted_group_with_runtime(
                    RecordingStateMachine {
                        applied_index: LogIndex(9),
                        ..RecordingStateMachine::default()
                    },
                    runtime,
                );
                let outcome = group
                    .begin_read_barrier_outcome(read_request(read_id, None))
                    .expect("barrier starts");
                let ReadProofOutcome::Granted { proof } = outcome else {
                    panic!(
                        "a fully applied state machine must satisfy every floor \
                         at or below the read index, got {outcome:?}"
                    );
                };
                oracle_assert!(
                    proof.required_applied_index <= proof.read_index,
                    "proof required {} above read index {} for entries \
                     {entries:?} snapshot {snapshot}",
                    proof.required_applied_index,
                    proof.read_index
                );
            }
        }
    }
}

/// Negative: "highest application entry in the cut" is sufficient only because
/// applies are ordered and gapless. That is enforced, not assumed — a state
/// machine whose cursor skipped an entry poisons the group on the existing
/// apply-floor path, before any barrier can be satisfied against that cursor.
#[test]
fn a_state_machine_that_skips_an_application_entry_poisons_before_a_read_can_grant() {
    let read_id = ReadId(96);
    let mut runtime = ScriptedRuntime::with_step_outputs([Vec::new()]);
    runtime.commit_index = LogIndex(6);
    runtime.application_entries = Some([LogIndex(3), LogIndex(5)].into_iter().collect());
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine {
            applied_index: LogIndex(3),
            // Claims to have consumed entry 5 without ever being handed it.
            reported_applied_index: Some(LogIndex(5)),
            ..RecordingStateMachine::default()
        },
        runtime,
    );

    let error = group
        .apply_raft_outputs(vec![apply_output(5, b"five", None)])
        .expect_err("a cursor that ran ahead of the applies is fatal");

    oracle_assert!(
        matches!(
            error,
            GroupError::ApplyEntryAlreadyApplied {
                entry_index: LogIndex(5),
                app_applied_index: LogIndex(5),
                ..
            }
        ),
        "the skipped entry must poison rather than be silently dropped, got {error:?}"
    );
    assert!(matches!(
        group.fatal_state(),
        GroupFatalState::Poisoned { .. }
    ));

    let refused = group
        .begin_read_barrier_outcome(read_request(read_id, None))
        .expect_err("a poisoned group can never grant a barrier");
    assert!(matches!(refused, GroupError::Poisoned { .. }));
}

fn assert_empty_report(report: &GroupStepReport<u64, Vec<u8>>) {
    assert_eq!(report.group_id, 7);
    assert!(report.peer_messages.is_empty());
    assert!(report.applied.is_empty());
    assert!(report.proposal_events.is_empty());
    assert!(report.read_events.is_empty());
    assert!(report.leadership_transfer_events.is_empty());
    assert!(report.snapshot_events.is_empty());
    assert!(report.membership_events.is_empty());
    assert_eq!(report.metrics, None);
}
