#![allow(clippy::wildcard_imports)]

mod support;

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
    assert_eq!(group.metrics().pending_reads, 0);
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
    assert_eq!(
        full.report.membership_events,
        vec![MembershipEvent::Applied {
            group_id: 7,
            index: LogIndex(5),
            term: Term(1),
            membership: updated_membership,
        }]
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
        .expect("local read succeeds");

    assert_eq!(
        outcome,
        ReadOutcome::Ready {
            result: Some(b"query".to_vec()),
            proof: None,
        }
    );
    assert!(group.runtime().step_inputs.is_empty());
    assert_eq!(group.metrics().pending_reads, 0);
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
        .expect("local read reports freshness");

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
        .expect("linearizable read succeeds");

    assert!(matches!(
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
    assert_eq!(group.metrics().pending_reads, 0);
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
        .expect("linearizable read starts");
    assert_eq!(
        first,
        ReadOutcome::Pending {
            read_id,
            peer_messages: Vec::new(),
        }
    );
    assert_eq!(group.metrics().pending_reads, 1);
    assert_eq!(group.runtime().step_inputs.len(), 1);

    let retry = group
        .read(read_helper_request(
            read_id,
            ReadConsistency::Linearizable,
            None,
        ))
        .expect("pending linearizable read can be retried");
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
    assert_eq!(group.metrics().pending_reads, 0);

    let completed = group
        .read(read_helper_request(
            read_id,
            ReadConsistency::Linearizable,
            None,
        ))
        .expect("completed proof drives state-machine read");
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
    assert_eq!(group.metrics().pending_reads, 0);
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
    assert_eq!(group.metrics().pending_reads, 0);

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
    assert_eq!(group.metrics().pending_reads, 0);

    let reuse = group
        .begin_read_barrier_outcome(read_request(read_id, None))
        .expect_err("canceled read id remains consumed");
    assert_non_monotonic_read_id(&reuse, read_id, read_id);
}
