#![allow(clippy::wildcard_imports)]

mod support;

use support::*;

#[test]
fn begin_read_barrier_rejects_helper_owned_pending_query_read_id() {
    let read_id = ReadId(64);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine {
            applied_index: LogIndex(4),
            ..RecordingStateMachine::default()
        },
        ScriptedRuntime::with_step_outputs([]),
    );
    let first = group
        .read(ReadRequest::Linearizable {
            group_id: 7,
            read_id,
            query: b"query".to_vec(),
            min_applied_index: Some(LogIndex(3)),
            context: b"helper".to_vec(),
        })
        .expect("helper read starts")
        .outcome;
    assert_eq!(
        first,
        ReadOutcome::Pending {
            read_id,
            peer_messages: Vec::new(),
        }
    );

    let error = group
        .begin_read_barrier_outcome(read_request(read_id, None))
        .expect_err("helper-owned read id is reserved");

    assert!(matches!(
        error,
        GroupError::DuplicateReadId {
            read_id: duplicate
        } if duplicate == read_id
    ));
    assert_read_metrics(&group, 1, 1, 0, 1);
    assert_eq!(
        group.runtime().step_inputs.len(),
        1,
        "reserved read id must not start another read-index round"
    );
}

#[test]
fn linearizable_read_retry_rejects_changed_pending_parameters() {
    let read_id = ReadId(62);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine {
            applied_index: LogIndex(4),
            ..RecordingStateMachine::default()
        },
        ScriptedRuntime::with_step_outputs([]),
    );

    let first = group
        .read(ReadRequest::Linearizable {
            group_id: 7,
            read_id,
            query: b"query".to_vec(),
            min_applied_index: None,
            context: b"same".to_vec(),
        })
        .expect("linearizable read starts")
        .outcome;
    assert_eq!(
        first,
        ReadOutcome::Pending {
            read_id,
            peer_messages: Vec::new(),
        }
    );

    let changed_freshness = group
        .read(ReadRequest::Linearizable {
            group_id: 7,
            read_id,
            query: b"query".to_vec(),
            min_applied_index: Some(LogIndex(5)),
            context: b"same".to_vec(),
        })
        .expect_err("changed freshness is rejected for duplicate read id");
    assert!(matches!(
        changed_freshness,
        GroupError::DuplicateReadId {
            read_id: duplicate
        } if duplicate == read_id
    ));

    let changed_context = group
        .read(ReadRequest::Linearizable {
            group_id: 7,
            read_id,
            query: b"query".to_vec(),
            min_applied_index: None,
            context: b"different".to_vec(),
        })
        .expect_err("changed context is rejected for duplicate read id");
    assert!(matches!(
        changed_context,
        GroupError::DuplicateReadId {
            read_id: duplicate
        } if duplicate == read_id
    ));

    assert_read_metrics(&group, 1, 1, 0, 1);
    assert_eq!(
        group.runtime().step_inputs.len(),
        1,
        "changed retries must not start another read-index round"
    );

    let retry = group
        .read(ReadRequest::Linearizable {
            group_id: 7,
            read_id,
            query: b"query".to_vec(),
            min_applied_index: None,
            context: b"same".to_vec(),
        })
        .expect("same parameters remain retryable")
        .outcome;
    assert_eq!(
        retry,
        ReadOutcome::Pending {
            read_id,
            peer_messages: Vec::new(),
        }
    );
}

#[test]
fn changed_retry_does_not_consume_completed_query_read() {
    let read_id = ReadId(63);
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

    group
        .apply_raft_outputs(vec![RaftOutput::ReadIndexGranted {
            read_id,
            read_index: LogIndex(4),
        }])
        .expect("normal progress completes proof");
    assert_read_metrics(&group, 0, 0, 1, 1);

    let changed = group
        .read(read_helper_request(
            read_id,
            ReadConsistency::Linearizable,
            Some(LogIndex(5)),
        ))
        .expect_err("changed completed retry is rejected");
    assert!(matches!(
        changed,
        GroupError::DuplicateReadId {
            read_id: duplicate
        } if duplicate == read_id
    ));
    assert_eq!(
        group.metrics().completed_query_reads,
        1,
        "changed retry must not consume completed proof"
    );

    let completed = group
        .read(read_helper_request(
            read_id,
            ReadConsistency::Linearizable,
            None,
        ))
        .expect("original parameters consume completed proof")
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
fn begin_read_barrier_rejects_completed_helper_read_without_consuming_proof() {
    let read_id = ReadId(65);
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

    group
        .apply_raft_outputs(vec![RaftOutput::ReadIndexGranted {
            read_id,
            read_index: LogIndex(4),
        }])
        .expect("normal progress completes proof");
    assert_read_metrics(&group, 0, 0, 1, 1);

    let duplicate = group
        .begin_read_barrier_outcome(read_request(read_id, None))
        .expect_err("completed helper proof reserves read id");
    assert!(matches!(
        duplicate,
        GroupError::DuplicateReadId {
            read_id: duplicate
        } if duplicate == read_id
    ));
    assert_eq!(
        group.metrics().completed_query_reads,
        1,
        "low-level duplicate must not consume completed proof"
    );
    assert_eq!(
        group.runtime().step_inputs.len(),
        1,
        "low-level duplicate must not start another read-index round"
    );

    let completed = group
        .read(read_helper_request(
            read_id,
            ReadConsistency::Linearizable,
            None,
        ))
        .expect("original helper retry still consumes completed proof")
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
fn freshness_unavailable_helper_read_reserves_until_cancel_read() {
    let read_id = ReadId(68);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine {
            applied_index: LogIndex(2),
            ..RecordingStateMachine::default()
        },
        ScriptedRuntime::with_step_outputs([vec![RaftOutput::ReadIndexGranted {
            read_id,
            read_index: LogIndex(4),
        }]]),
    );

    let outcome = group
        .read(read_helper_request(
            read_id,
            ReadConsistency::Linearizable,
            None,
        ))
        .expect("linearizable read starts and observes freshness gap")
        .outcome;
    assert_eq!(
        outcome,
        ReadOutcome::LinearizableFreshnessUnavailable {
            read_id,
            required_applied_index: LogIndex(4),
            local_applied_index: LogIndex(2),
        }
    );
    assert_read_metrics(&group, 1, 1, 0, 1);

    let duplicate = group
        .begin_read_barrier_outcome(read_request(read_id, None))
        .expect_err("freshness-stalled helper read reserves read id");
    assert!(matches!(
        duplicate,
        GroupError::DuplicateReadId {
            read_id: duplicate
        } if duplicate == read_id
    ));
    assert_eq!(
        group.runtime().step_inputs.len(),
        1,
        "duplicate barrier must not start another read-index round"
    );

    assert!(group.cancel_read(read_id));
    assert_read_metrics(&group, 0, 0, 0, 0);

    let same_id_error = group
        .begin_read_barrier_outcome(read_request(read_id, None))
        .expect_err("canceled read id remains consumed");
    assert_non_monotonic_read_id(&same_id_error, read_id, read_id);

    let higher_id = ReadId(69);
    let fresh = group
        .begin_read_barrier_outcome(read_request(higher_id, None))
        .expect("higher read id can start after cancellation");
    assert_eq!(
        fresh,
        ReadProofOutcome::Pending {
            read_id: higher_id,
            peer_messages: Vec::new(),
        }
    );
    assert_read_metrics(&group, 1, 0, 0, 1);
    assert_eq!(
        group.runtime().step_inputs.len(),
        2,
        "higher barrier starts a new read-index round"
    );

    let late = group
        .apply_raft_outputs(vec![
            RaftOutput::ReadIndexGranted {
                read_id,
                read_index: LogIndex(4),
            },
            RaftOutput::ReadIndexRejected {
                read_id,
                reason: ReadIndexRejection::NotLeader {
                    role: Role::Follower,
                    term: Term(3),
                },
            },
            RaftOutput::ReadIndexCanceled {
                read_id,
                reason: ReadIndexCancelReason::LeaderStateReset,
            },
        ])
        .expect("late read outputs are ignored after higher read starts");
    assert!(late.read_events.is_empty());
    assert_read_metrics(&group, 1, 0, 0, 1);
}

#[test]
fn canceled_linearizable_read_helper_clears_query_state() {
    let read_id = ReadId(57);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine {
            applied_index: LogIndex(4),
            ..RecordingStateMachine::default()
        },
        ScriptedRuntime::with_step_outputs([Vec::new(), Vec::new()]),
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
    assert_read_metrics(&group, 1, 1, 0, 1);

    let report = group
        .apply_raft_outputs(vec![RaftOutput::ReadIndexCanceled {
            read_id,
            reason: ReadIndexCancelReason::LeaderStateReset,
        }])
        .expect("external cancellation is reported");
    assert!(report.read_events.iter().any(|event| matches!(
        event,
        ReadEvent::Canceled {
            read_id: event_read_id,
            reason: ReadIndexCancelReason::LeaderStateReset,
            ..
        } if *event_read_id == read_id
    )));
    assert_read_metrics(&group, 0, 0, 0, 0);

    let same_id_retry = group
        .read(read_helper_request(
            read_id,
            ReadConsistency::Linearizable,
            None,
        ))
        .expect_err("runtime-canceled read id remains consumed");
    assert_non_monotonic_read_id(&same_id_retry, read_id, read_id);

    let higher_id = ReadId(58);
    let retry = group
        .read(read_helper_request(
            higher_id,
            ReadConsistency::Linearizable,
            None,
        ))
        .expect("higher read id starts cleanly after cancellation")
        .outcome;
    assert_eq!(
        retry,
        ReadOutcome::Pending {
            read_id: higher_id,
            peer_messages: Vec::new(),
        }
    );
    assert_eq!(
        group.runtime().step_inputs.len(),
        2,
        "retry after cancellation must start a new read-index round"
    );
}

#[test]
fn cancel_read_drops_pending_helper_state_and_ignores_late_cancellation() {
    let read_id = ReadId(66);
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
    assert_read_metrics(&group, 1, 1, 0, 1);

    assert!(group.cancel_read(read_id));
    assert_read_metrics(&group, 0, 0, 0, 0);
    assert!(!group.cancel_read(read_id));

    let report = group
        .apply_raft_outputs(vec![RaftOutput::ReadIndexCanceled {
            read_id,
            reason: ReadIndexCancelReason::LeaderStateReset,
        }])
        .expect("late cancellation is ignored after local cleanup");
    assert!(report.read_events.is_empty());
}

#[test]
fn drop_completed_read_removes_cached_proof_and_allows_fresh_retry() {
    let read_id = ReadId(67);
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

    group
        .apply_raft_outputs(vec![RaftOutput::ReadIndexGranted {
            read_id,
            read_index: LogIndex(4),
        }])
        .expect("normal progress completes proof");
    assert_read_metrics(&group, 0, 0, 1, 1);

    assert!(group.drop_completed_read(read_id));
    assert_read_metrics(&group, 0, 0, 0, 0);
    assert!(!group.drop_completed_read(read_id));

    let same_id_retry = group
        .read(read_helper_request(
            read_id,
            ReadConsistency::Linearizable,
            None,
        ))
        .expect_err("dropped completed proof leaves read id consumed");
    assert_non_monotonic_read_id(&same_id_retry, read_id, read_id);

    let higher_id = ReadId(68);
    let retry = group
        .read(read_helper_request(
            higher_id,
            ReadConsistency::Linearizable,
            None,
        ))
        .expect("higher read id may start after dropping completed proof")
        .outcome;
    assert_eq!(
        retry,
        ReadOutcome::Pending {
            read_id: higher_id,
            peer_messages: Vec::new(),
        }
    );
    assert_eq!(
        group.runtime().step_inputs.len(),
        2,
        "dropping completed proof lets the next retry start a fresh barrier"
    );
}

#[test]
fn read_barrier_waits_until_apply_catches_up() {
    let read_id = ReadId(51);
    let runtime = ScriptedRuntime::with_step_outputs([vec![RaftOutput::ReadIndexGranted {
        read_id,
        read_index: LogIndex(4),
    }]]);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine {
            applied_index: LogIndex(2),
            ..RecordingStateMachine::default()
        },
        runtime,
    );

    let outcome = group
        .begin_read_barrier_outcome(read_request(read_id, None))
        .expect("read barrier starts");
    assert_eq!(
        outcome,
        ReadProofOutcome::FreshnessUnavailable {
            read_id,
            required_applied_index: LogIndex(4),
            local_applied_index: LogIndex(2),
        }
    );
    assert_eq!(group.metrics().pending_reads, 1);

    let report = group
        .apply_raft_outputs(vec![apply_output(4, b"catch-up", None)])
        .expect("catch-up entry applies");
    assert!(report.read_events.iter().any(|event| matches!(
        event,
        ReadEvent::Granted {
            read_id: event_read_id,
            proof:
                ReadProof {
                    read_index: LogIndex(4),
                    required_applied_index: LogIndex(4),
                    local_applied_index: LogIndex(4),
                    ..
                },
        } if *event_read_id == read_id
    )));
    assert_eq!(group.metrics().pending_reads, 0);
}

#[test]
fn follower_read_barrier_rejection_includes_leader_hint() {
    let read_id = ReadId(52);
    let mut runtime = ScriptedRuntime::with_step_outputs([vec![RaftOutput::ReadIndexRejected {
        read_id,
        reason: ReadIndexRejection::NotLeader {
            role: Role::Follower,
            term: Term(3),
        },
    }]]);
    runtime.leader_hint = Some(NodeId(2));
    let mut group = scripted_group_with_runtime(RecordingStateMachine::default(), runtime);

    let outcome = group
        .begin_read_barrier_outcome(read_request(read_id, None))
        .expect("read rejection is reported");
    assert_eq!(
        outcome,
        ReadProofOutcome::Rejected {
            read_id,
            reason: ReadIndexRejection::NotLeader {
                role: Role::Follower,
                term: Term(3),
            },
            leader_hint: Some(NodeId(2)),
        }
    );
    assert_eq!(group.metrics().pending_reads, 0);
}

#[test]
fn canceled_read_barrier_clears_pending_read() {
    let read_id = ReadId(53);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine::default(),
        ScriptedRuntime::with_step_outputs([vec![]]),
    );
    begin_pending_read_barrier(&mut group, read_id, None);

    let report = group
        .apply_raft_outputs(vec![RaftOutput::ReadIndexCanceled {
            read_id,
            reason: ReadIndexCancelReason::LeadershipLost,
        }])
        .expect("read cancel output maps to read event");

    assert_eq!(
        report.read_events,
        vec![ReadEvent::Canceled {
            read_id,
            reason: ReadIndexCancelReason::LeadershipLost,
            leader_hint: Some(NodeId(1)),
        }]
    );
    assert_eq!(group.metrics().pending_reads, 0);
}

#[test]
fn begin_read_barrier_reports_canceled_outcome() {
    let read_id = ReadId(54);
    let mut runtime = ScriptedRuntime::with_step_outputs([vec![RaftOutput::ReadIndexCanceled {
        read_id,
        reason: ReadIndexCancelReason::LeadershipLost,
    }]]);
    runtime.leader_hint = Some(NodeId(2));
    let mut group = scripted_group_with_runtime(RecordingStateMachine::default(), runtime);

    let outcome = group
        .begin_read_barrier_outcome(read_request(read_id, None))
        .expect("read cancellation is reported");
    assert_eq!(
        outcome,
        ReadProofOutcome::Canceled {
            read_id,
            reason: ReadIndexCancelReason::LeadershipLost,
            leader_hint: Some(NodeId(2)),
        }
    );
    assert_eq!(group.metrics().pending_reads, 0);
}

#[test]
fn poisoned_group_rejects_read_barriers() {
    let mut group = scripted_group(RecordingStateMachine {
        apply_mode: ApplyMode::Fail,
        ..RecordingStateMachine::default()
    });
    let _ = group
        .apply_raft_outputs(vec![apply_output(2, b"bad", Some(LocalProposalId(60)))])
        .expect_err("apply failure poisons group");

    let error = group
        .begin_read_barrier_outcome(read_request(ReadId(60), None))
        .expect_err("poisoned group rejects read barriers");
    assert!(matches!(error, GroupError::Poisoned { .. }));
}
