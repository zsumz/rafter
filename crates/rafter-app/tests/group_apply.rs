#![allow(clippy::wildcard_imports)]

mod support;

use support::*;

#[test]
fn apply_failure_poisons_group_and_rejects_writes() {
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine {
            apply_mode: ApplyMode::Fail,
            ..RecordingStateMachine::default()
        },
        ScriptedRuntime::with_step_outputs([vec![append_output(LocalProposalId(20), 2)], vec![]]),
    );
    let client_request_id = Some(ClientRequestId {
        client_id: 5,
        sequence: 1,
    });
    begin_pending_proposal(&mut group, LocalProposalId(20), client_request_id, 2);
    begin_pending_read_barrier(&mut group, ReadId(20), None);

    let error = group
        .apply_raft_outputs(vec![apply_output(2, b"bad", Some(LocalProposalId(20)))])
        .expect_err("apply failure is fatal");
    assert!(matches!(
        error,
        GroupError::StateMachine {
            operation: StateMachineOperation::ApplyBatch,
            ref source,
        } if **source == RecordingStateMachineError::Apply
    ));
    assert!(matches!(
        group.fatal_state(),
        GroupFatalState::Poisoned { .. }
    ));
    assert!(matches!(
        group.metrics().fatal_state,
        GroupFatalState::Poisoned { .. }
    ));
    assert_eq!(group.metrics().pending_proposals, 0);
    assert_eq!(group.metrics().pending_reads, 0);
    assert_eq!(
        group.poisoned_waiters(),
        &PoisonedWaiters {
            proposals: vec![(
                LocalProposalId(20),
                Some(ClientRequestId {
                    client_id: 5,
                    sequence: 1,
                }),
            )],
            reads: vec![ReadId(20)],
        }
    );
    assert_eq!(
        group.drain_poisoned_waiters(),
        PoisonedWaiters {
            proposals: vec![(
                LocalProposalId(20),
                Some(ClientRequestId {
                    client_id: 5,
                    sequence: 1,
                }),
            )],
            reads: vec![ReadId(20)],
        }
    );
    assert!(group.drain_poisoned_waiters().is_empty());

    let write_error = group
        .begin_proposal_outcome(Proposal {
            local_proposal_id: LocalProposalId(21),
            client_request_id: None,
            command: b"retry".to_vec(),
        })
        .expect_err("poisoned group rejects new writes");
    assert!(matches!(write_error, GroupError::Poisoned { .. }));
}

#[test]
fn poisoned_group_rejects_direct_apply_outputs_before_handling_them() {
    let mut group = scripted_group(RecordingStateMachine {
        apply_mode: ApplyMode::Fail,
        ..RecordingStateMachine::default()
    });
    let _ = group
        .apply_raft_outputs(vec![apply_output(2, b"bad", Some(LocalProposalId(22)))])
        .expect_err("apply failure poisons group");
    assert!(matches!(
        group.fatal_state(),
        GroupFatalState::Poisoned { .. }
    ));

    let error = group
        .apply_raft_outputs(vec![RaftOutput::ReadIndexGranted {
            read_id: ReadId(22),
            read_index: LogIndex(2),
        }])
        .expect_err("poisoned group rejects direct output handling");

    assert!(matches!(error, GroupError::Poisoned { .. }));
    assert_eq!(group.metrics().pending_reads, 0);
}

#[test]
fn apply_result_count_mismatch_poisons_group() {
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine {
            apply_mode: ApplyMode::DropLastResult,
            ..RecordingStateMachine::default()
        },
        ScriptedRuntime::with_step_outputs([
            vec![append_output(LocalProposalId(30), 2)],
            vec![RaftOutput::ReadIndexGranted {
                read_id: ReadId(30),
                read_index: LogIndex(3),
            }],
        ]),
    );
    begin_pending_proposal(&mut group, LocalProposalId(30), None, 2);
    begin_pending_read_barrier(&mut group, ReadId(30), None);

    let error = group
        .apply_raft_outputs(vec![
            apply_output(2, b"one", Some(LocalProposalId(30))),
            apply_output(3, b"two", Some(LocalProposalId(31))),
        ])
        .expect_err("malformed apply results are fatal");
    assert!(matches!(
        error,
        GroupError::ApplyResultCountMismatch {
            expected: 2,
            actual: 1
        }
    ));
    assert!(matches!(
        group.fatal_state(),
        GroupFatalState::Poisoned { .. }
    ));
    assert!(matches!(
        group.metrics().fatal_state,
        GroupFatalState::Poisoned { .. }
    ));
    assert_eq!(group.metrics().pending_proposals, 0);
    assert_eq!(group.metrics().pending_reads, 0);
}

#[test]
fn apply_result_wrong_index_poisons_group() {
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine {
            apply_mode: ApplyMode::WrongIndex,
            ..RecordingStateMachine::default()
        },
        ScriptedRuntime::with_step_outputs([vec![append_output(LocalProposalId(30), 2)]]),
    );
    begin_pending_proposal(&mut group, LocalProposalId(30), None, 2);

    let error = group
        .apply_raft_outputs(vec![apply_output(2, b"one", Some(LocalProposalId(30)))])
        .expect_err("wrong apply index is fatal");

    assert!(matches!(
        error,
        GroupError::ApplyResultMetadataMismatch {
            expected_index: LogIndex(2),
            actual_index: LogIndex(3),
            expected_term: Term(1),
            actual_term: Term(1),
            expected_local_proposal_id: Some(LocalProposalId(30)),
            actual_local_proposal_id: Some(LocalProposalId(30)),
        }
    ));
    assert!(matches!(
        group.fatal_state(),
        GroupFatalState::Poisoned { .. }
    ));
    assert_eq!(group.metrics().pending_proposals, 0);
}

#[test]
fn apply_result_wrong_term_poisons_group() {
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine {
            apply_mode: ApplyMode::WrongTerm,
            ..RecordingStateMachine::default()
        },
        ScriptedRuntime::with_step_outputs([vec![append_output(LocalProposalId(31), 2)]]),
    );
    begin_pending_proposal(&mut group, LocalProposalId(31), None, 2);

    let error = group
        .apply_raft_outputs(vec![apply_output(2, b"one", Some(LocalProposalId(31)))])
        .expect_err("wrong apply term is fatal");

    assert!(matches!(
        error,
        GroupError::ApplyResultMetadataMismatch {
            expected_index: LogIndex(2),
            actual_index: LogIndex(2),
            expected_term: Term(1),
            actual_term: Term(2),
            expected_local_proposal_id: Some(LocalProposalId(31)),
            actual_local_proposal_id: Some(LocalProposalId(31)),
        }
    ));
    assert!(matches!(
        group.fatal_state(),
        GroupFatalState::Poisoned { .. }
    ));
    assert_eq!(group.metrics().pending_proposals, 0);
}

#[test]
fn apply_result_wrong_local_proposal_id_poisons_group() {
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine {
            apply_mode: ApplyMode::WrongLocalProposalId,
            ..RecordingStateMachine::default()
        },
        ScriptedRuntime::with_step_outputs([vec![append_output(LocalProposalId(32), 2)]]),
    );
    begin_pending_proposal(&mut group, LocalProposalId(32), None, 2);

    let error = group
        .apply_raft_outputs(vec![apply_output(2, b"one", Some(LocalProposalId(32)))])
        .expect_err("wrong local proposal id is fatal");

    assert!(matches!(
        error,
        GroupError::ApplyResultMetadataMismatch {
            expected_index: LogIndex(2),
            actual_index: LogIndex(2),
            expected_term: Term(1),
            actual_term: Term(1),
            expected_local_proposal_id: Some(LocalProposalId(32)),
            actual_local_proposal_id: Some(LocalProposalId(999)),
        }
    ));
    assert!(matches!(
        group.fatal_state(),
        GroupFatalState::Poisoned { .. }
    ));
    assert_eq!(group.metrics().pending_proposals, 0);
}

#[test]
fn apply_success_with_stale_app_applied_index_poisons_group() {
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine {
            reported_applied_index: Some(LogIndex(1)),
            ..RecordingStateMachine::default()
        },
        ScriptedRuntime::with_step_outputs([vec![append_output(LocalProposalId(33), 2)]]),
    );
    begin_pending_proposal(&mut group, LocalProposalId(33), None, 2);

    let error = group
        .apply_raft_outputs(vec![apply_output(2, b"one", Some(LocalProposalId(33)))])
        .expect_err("stale applied index after apply is fatal");

    assert!(matches!(
        error,
        GroupError::AppliedIndexBehind {
            required: LogIndex(2),
            actual: LogIndex(1),
        }
    ));
    assert!(matches!(
        group.fatal_state(),
        GroupFatalState::Poisoned { .. }
    ));
    assert_eq!(group.metrics().pending_proposals, 0);
}

#[test]
fn already_applied_entry_is_rejected_before_apply_batch() {
    let mut group = scripted_group(RecordingStateMachine {
        applied_index: LogIndex(5),
        ..RecordingStateMachine::default()
    });

    let error = group
        .apply_raft_outputs(vec![apply_output(5, b"already-applied", None)])
        .expect_err("replaying an app-applied entry is fatal");

    assert!(matches!(
        error,
        GroupError::ApplyEntryAlreadyApplied {
            entry_index: LogIndex(5),
            app_applied_index: LogIndex(5),
            group_applied_index: LogIndex::ZERO,
        }
    ));
    assert!(matches!(
        group.fatal_state(),
        GroupFatalState::Poisoned { .. }
    ));
    assert!(group.state_machine().batches.is_empty());
    assert!(group.state_machine().applied.is_empty());
}

#[test]
fn recovered_applied_floor_accepts_next_unapplied_entry() {
    let mut group = RaftGroup::with_applied_index(
        7,
        NodeId(1),
        ScriptedRuntime::with_terms([(LogIndex(6), Term(1))]),
        RecordingStateMachine {
            applied_index: LogIndex(5),
            ..RecordingStateMachine::default()
        },
        LogIndex(5),
    );

    let report = group
        .apply_raft_outputs(vec![apply_output(6, b"next", None)])
        .expect("next entry above the app floor applies");

    assert!(matches!(group.fatal_state(), GroupFatalState::Healthy));
    assert_eq!(group.state_machine().batches, vec![vec![LogIndex(6)]]);
    assert_eq!(group.state_machine().applied, vec![b"next".to_vec()]);
    assert_eq!(report.applied.len(), 1);
    assert_eq!(report.applied[0].index, LogIndex(6));
}

#[test]
fn apply_snapshot_installs_state_machine_snapshot_and_reports_event() {
    let snapshot = test_snapshot(8);
    let mut group = scripted_group(RecordingStateMachine::default());

    let report = group
        .apply_raft_outputs(vec![RaftOutput::ApplySnapshot {
            snapshot: snapshot.clone(),
        }])
        .expect("snapshot install succeeds");

    assert_eq!(
        report.snapshot_events,
        vec![SnapshotEvent::Apply {
            group_id: 7,
            snapshot: snapshot.clone(),
        }]
    );
    assert_eq!(group.state_machine().applied_index, LogIndex(8));
    assert_eq!(group.state_machine().installed_snapshots.len(), 1);
    let installed = &group.state_machine().installed_snapshots[0];
    assert_eq!(installed.applied_index, LogIndex(8));
    assert!(installed.payload.is_empty());
    assert_eq!(installed.raft_snapshot, Some(snapshot));
    assert!(matches!(group.fatal_state(), GroupFatalState::Healthy));
    assert_eq!(
        report
            .metrics
            .as_ref()
            .expect("step report has metrics")
            .applied_index,
        LogIndex(8)
    );
}

#[test]
fn apply_snapshot_failure_poisons_group_and_clears_waiters() {
    let snapshot = test_snapshot(9);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine {
            fail_install_snapshot: true,
            ..RecordingStateMachine::default()
        },
        ScriptedRuntime::with_step_outputs([
            vec![append_output(LocalProposalId(40), 2)],
            vec![RaftOutput::ReadIndexGranted {
                read_id: ReadId(40),
                read_index: LogIndex(9),
            }],
        ]),
    );
    begin_pending_proposal(&mut group, LocalProposalId(40), None, 2);
    begin_pending_read_barrier(&mut group, ReadId(40), Some(LogIndex(9)));

    let error = group
        .apply_raft_outputs(vec![RaftOutput::ApplySnapshot { snapshot }])
        .expect_err("failed snapshot install is fatal");

    assert!(matches!(
        error,
        GroupError::StateMachine {
            operation: StateMachineOperation::InstallSnapshot,
            ref source,
        } if **source == RecordingStateMachineError::InstallSnapshot
    ));
    assert!(matches!(
        group.fatal_state(),
        GroupFatalState::Poisoned { .. }
    ));
    assert_eq!(group.metrics().pending_proposals, 0);
    assert_eq!(group.metrics().pending_reads, 0);
}

#[test]
fn snapshot_install_with_stale_app_applied_index_poisons_group() {
    let snapshot = test_snapshot(9);
    let mut group = scripted_group(RecordingStateMachine {
        reported_applied_index: Some(LogIndex(8)),
        ..RecordingStateMachine::default()
    });

    let error = group
        .apply_raft_outputs(vec![RaftOutput::ApplySnapshot { snapshot }])
        .expect_err("stale applied index after snapshot install is fatal");

    assert!(matches!(
        error,
        GroupError::AppliedIndexBehind {
            required: LogIndex(9),
            actual: LogIndex(8),
        }
    ));
    assert!(matches!(
        group.fatal_state(),
        GroupFatalState::Poisoned { .. }
    ));
}

#[test]
fn malformed_snapshot_output_poisons_group_before_install() {
    let mut snapshot = test_snapshot(9);
    snapshot.metadata.last_included_index = LogIndex::ZERO;
    let mut group = scripted_group(RecordingStateMachine::default());

    let error = group
        .apply_raft_outputs(vec![RaftOutput::ApplySnapshot { snapshot }])
        .expect_err("malformed snapshot output is fatal");

    assert!(matches!(
        error,
        GroupError::MalformedSnapshot { ref reason }
            if reason == "snapshot last included index is zero"
    ));
    assert!(matches!(
        group.fatal_state(),
        GroupFatalState::Poisoned { .. }
    ));
    assert!(group.state_machine().installed_snapshots.is_empty());
}

#[test]
fn snapshot_chunk_outputs_are_reported_without_poisoning() {
    let snapshot = test_snapshot(10);
    let staged = staged_snapshot_chunk(&snapshot);
    let send = snapshot_chunk_send(&snapshot);
    let mut group = scripted_group(RecordingStateMachine::default());

    let report = group
        .apply_raft_outputs(vec![
            RaftOutput::StageSnapshotChunk {
                chunk: staged.clone(),
            },
            RaftOutput::SendSnapshotChunk {
                to: NodeId(2),
                chunk: send.clone(),
            },
        ])
        .expect("snapshot chunk outputs are reportable");

    assert_eq!(
        report.snapshot_events,
        vec![
            SnapshotEvent::StageChunk {
                group_id: 7,
                chunk: staged,
            },
            SnapshotEvent::SendChunk {
                group_id: 7,
                to: NodeId(2),
                chunk: send,
            },
        ]
    );
    assert!(matches!(group.fatal_state(), GroupFatalState::Healthy));
    assert!(report
        .metrics
        .as_ref()
        .is_some_and(|metrics| metrics.fatal_state == GroupFatalState::Healthy));
}

/// The readiness predicate is runtime-derived precisely so it stays false while
/// a restarted node still holds recovery outputs the caller has not applied — a
/// floor tracked from applies the group has *seen* would report ready here.
#[test]
fn readiness_predicate_is_false_until_recovery_outputs_are_applied() {
    use rafter_runtime::DurableRaftNode;
    use rafter_storage::{
        InMemoryRaftHardStateStore, InMemoryRaftLogSegment, InMemoryRaftSnapshotStore,
        PersistedRaftLogEntry, RaftHardState, RaftHardStateStore, RaftLogSegment,
    };

    let mut hard_state_store = InMemoryRaftHardStateStore::new();
    hard_state_store
        .write_hard_state(RaftHardState {
            current_term: Term(1),
            voted_for: None,
            commit_index: LogIndex(3),
            committed_configuration: None,
        })
        .expect("durable commit floor writes");
    let mut log_segment = InMemoryRaftLogSegment::new();
    log_segment
        .append_entries(&[
            PersistedRaftLogEntry::application(LogIndex(1), Term(1), b"one".to_vec()),
            PersistedRaftLogEntry::application(LogIndex(2), Term(1), b"two".to_vec()),
            PersistedRaftLogEntry::application(LogIndex(3), Term(1), b"three".to_vec()),
        ])
        .expect("committed application entries persist");
    let (runtime, recovery_outputs) =
        DurableRaftNode::recover_with_storage_and_snapshot_store_applied_through(
            NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 1)
                .expect("test node config is valid"),
            hard_state_store,
            log_segment,
            InMemoryRaftSnapshotStore::new(),
            LogIndex(1),
        )
        .expect("runtime recovers above the applied floor")
        .into_parts();

    let mut group = RaftGroup::with_applied_index(
        7,
        NodeId(1),
        runtime,
        RecordingStateMachine {
            applied_index: LogIndex(1),
            ..RecordingStateMachine::default()
        },
        LogIndex(1),
    );

    assert_eq!(group.committed_application_index(), LogIndex(3));
    assert!(
        group.state_machine().applied_index().expect("floor reads")
            < group.committed_application_index(),
        "a replica holding undrained recovery outputs must not read as ready"
    );

    group
        .apply_raft_outputs(recovery_outputs)
        .expect("recovery outputs apply");

    assert!(
        group.state_machine().applied_index().expect("floor reads")
            >= group.committed_application_index(),
        "the replica is ready once the recovery outputs are applied"
    );
    assert_eq!(group.state_machine().applied_index(), Ok(LogIndex(3)));
}

/// Poison does not change the runtime's answer, and a poisoned group will never
/// apply again — so a readiness gate that consults only this value would hold a
/// dead replica open. The doc says to check `fatal_state` too; this pins the
/// half that is easy to assume away.
#[test]
fn committed_application_index_is_reported_by_a_poisoned_group() {
    let mut runtime = ScriptedRuntime::with_step_outputs([]);
    runtime.commit_index = LogIndex(9);
    // A committed tail that is not an application entry: the highest committed
    // application entry sits at 8, which is what a readiness gate must compare.
    runtime.application_entries = Some([LogIndex(8)].into_iter().collect());
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine {
            fail_decode: true,
            ..RecordingStateMachine::default()
        },
        runtime,
    );
    assert_eq!(group.committed_application_index(), LogIndex(8));

    let _ = group
        .apply_raft_outputs(vec![apply_output(2, b"bad", Some(LocalProposalId(40)))])
        .expect_err("decode failure is fatal");

    assert!(matches!(
        group.fatal_state(),
        GroupFatalState::Poisoned { .. }
    ));
    assert_eq!(group.committed_application_index(), LogIndex(8));
}

#[test]
fn decode_failure_poisons_group() {
    let mut group = scripted_group(RecordingStateMachine {
        fail_decode: true,
        ..RecordingStateMachine::default()
    });

    let error = group
        .apply_raft_outputs(vec![apply_output(2, b"bad", Some(LocalProposalId(40)))])
        .expect_err("decode failure is fatal");
    assert!(matches!(
        error,
        GroupError::StateMachine {
            operation: StateMachineOperation::DecodeCommand,
            ref source,
        } if **source == RecordingStateMachineError::Decode
    ));
    assert!(matches!(
        group.fatal_state(),
        GroupFatalState::Poisoned { .. }
    ));
}
