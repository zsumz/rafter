#![allow(clippy::wildcard_imports)]

mod support;

use support::*;

#[test]
fn begin_proposal_rejects_duplicate_pending_local_proposal_id() {
    let proposal_id = LocalProposalId(80);
    let client_request_id = Some(ClientRequestId {
        client_id: 8,
        sequence: 1,
    });
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine::default(),
        ScriptedRuntime::with_step_outputs([vec![append_output(proposal_id, 3)]]),
    );
    begin_pending_proposal(&mut group, proposal_id, client_request_id, 3);

    let error = group
        .begin_proposal_outcome(Proposal {
            local_proposal_id: proposal_id,
            client_request_id: Some(ClientRequestId {
                client_id: 8,
                sequence: 2,
            }),
            command: b"duplicate".to_vec(),
        })
        .expect_err("duplicate pending proposal ID is rejected");

    assert!(matches!(
        error,
        GroupError::NonMonotonicLocalProposalId {
            local_proposal_id,
            last_seen_local_proposal_id,
        } if local_proposal_id == proposal_id
            && last_seen_local_proposal_id == proposal_id
    ));
    assert_eq!(group.metrics().pending_proposals, 1);
    assert_eq!(group.runtime().step_inputs.len(), 1);

    let report = group
        .apply_raft_outputs(vec![apply_output(2, b"original", Some(proposal_id))])
        .expect("original pending proposal can still complete");
    assert_eq!(
        report.proposal_events,
        vec![ProposalEvent::Applied {
            local_proposal_id: proposal_id,
            index: LogIndex(2),
            term: Term(1),
            result: b"original".to_vec(),
        }]
    );
    assert_eq!(group.metrics().pending_proposals, 0);

    let reuse_error = group
        .begin_proposal_outcome(Proposal {
            local_proposal_id: proposal_id,
            client_request_id: None,
            command: b"reused".to_vec(),
        })
        .expect_err("terminal proposal ID remains single-use");
    assert!(matches!(
        reuse_error,
        GroupError::NonMonotonicLocalProposalId {
            local_proposal_id,
            last_seen_local_proposal_id,
        } if local_proposal_id == proposal_id
            && last_seen_local_proposal_id == proposal_id
    ));
    assert_eq!(group.runtime().step_inputs.len(), 1);
}

#[test]
fn group_step_proposal_rejects_duplicate_pending_local_proposal_id() {
    let proposal_id = LocalProposalId(81);
    let client_request_id = Some(ClientRequestId {
        client_id: 8,
        sequence: 10,
    });
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine::default(),
        ScriptedRuntime::with_step_outputs([vec![append_output(proposal_id, 2)]]),
    );
    begin_pending_proposal(&mut group, proposal_id, client_request_id, 2);

    let error = group
        .step(GroupInput::Proposal {
            proposal: Proposal {
                local_proposal_id: proposal_id,
                client_request_id: Some(ClientRequestId {
                    client_id: 8,
                    sequence: 11,
                }),
                command: b"duplicate".to_vec(),
            },
        })
        .expect_err("duplicate pending proposal ID is rejected");

    assert!(matches!(
        error,
        GroupError::NonMonotonicLocalProposalId {
            local_proposal_id,
            last_seen_local_proposal_id,
        } if local_proposal_id == proposal_id
            && last_seen_local_proposal_id == proposal_id
    ));
    assert_eq!(group.metrics().pending_proposals, 1);
    assert_eq!(group.runtime().step_inputs.len(), 1);
    assert_eq!(group.metrics().pending_proposals, 1);
}

#[test]
fn lower_local_proposal_id_is_rejected_after_higher_seen() {
    let first_id = LocalProposalId(90);
    let lower_id = LocalProposalId(89);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine::default(),
        ScriptedRuntime::with_step_outputs([vec![RaftOutput::LocalProposalAppended {
            proposal_id: first_id,
            index: LogIndex(2),
            term: Term(1),
        }]]),
    );

    group
        .begin_proposal_outcome(Proposal {
            local_proposal_id: first_id,
            client_request_id: None,
            command: b"first".to_vec(),
        })
        .expect("first higher ID starts");

    let error = group
        .begin_proposal_outcome(Proposal {
            local_proposal_id: lower_id,
            client_request_id: None,
            command: b"lower".to_vec(),
        })
        .expect_err("lower proposal ID is rejected by monotonic policy");

    assert!(matches!(
        error,
        GroupError::NonMonotonicLocalProposalId {
            local_proposal_id,
            last_seen_local_proposal_id,
        } if local_proposal_id == lower_id
            && last_seen_local_proposal_id == first_id
    ));
    assert_eq!(group.runtime().step_inputs.len(), 1);
    assert_eq!(group.metrics().pending_proposals, 1);
}

#[test]
fn command_encode_failure_does_not_consume_local_proposal_id() {
    let proposal_id = LocalProposalId(91);
    let mut group = scripted_group(RecordingStateMachine {
        fail_encode: true,
        ..RecordingStateMachine::default()
    });

    let error = group
        .begin_proposal_outcome(Proposal {
            local_proposal_id: proposal_id,
            client_request_id: None,
            command: b"encode fails".to_vec(),
        })
        .expect_err("encode failure rejects proposal");

    assert!(matches!(
        error,
        GroupError::StateMachine {
            operation: StateMachineOperation::EncodeCommand,
            ref source,
        } if **source == RecordingStateMachineError::Encode
    ));
    assert_eq!(group.local_proposal_id_watermark(), None);
    assert_eq!(group.metrics().pending_proposals, 0);
    assert!(group.runtime().step_inputs.is_empty());

    group.state_machine_mut().fail_encode = false;
    group
        .begin_proposal_outcome(Proposal {
            local_proposal_id: proposal_id,
            client_request_id: None,
            command: b"retry after encode fixed".to_vec(),
        })
        .expect_err("runtime returns no lifecycle event, but ID was reusable after encode failure");
    assert_eq!(group.local_proposal_id_watermark(), Some(proposal_id));
    assert_eq!(group.runtime().step_inputs.len(), 1);
}

#[test]
fn runtime_step_error_consumes_local_proposal_id() {
    let proposal_id = LocalProposalId(92);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine::default(),
        ScriptedRuntime::with_step_errors([TestRuntimeError::Forced]),
    );

    let error = group
        .begin_proposal_outcome(Proposal {
            local_proposal_id: proposal_id,
            client_request_id: None,
            command: b"runtime fails".to_vec(),
        })
        .expect_err("runtime error is returned");

    assert!(matches!(error, GroupError::Runtime(_)));
    assert_eq!(group.local_proposal_id_watermark(), Some(proposal_id));
    assert_eq!(group.metrics().pending_proposals, 0);
    assert_eq!(group.runtime().step_inputs.len(), 1);

    let reuse = group
        .begin_proposal_outcome(Proposal {
            local_proposal_id: proposal_id,
            client_request_id: None,
            command: b"reuse after runtime error".to_vec(),
        })
        .expect_err("runtime-submitted local proposal ID is consumed");
    assert!(matches!(
        reuse,
        GroupError::NonMonotonicLocalProposalId {
            local_proposal_id,
            last_seen_local_proposal_id,
        } if local_proposal_id == proposal_id
            && last_seen_local_proposal_id == proposal_id
    ));
}

#[test]
fn begin_proposal_batch_submits_one_runtime_batch_and_preserves_order() {
    let first_id = LocalProposalId(100);
    let second_id = LocalProposalId(101);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine::default(),
        ScriptedRuntime::with_step_outputs([vec![
            append_output(first_id, 2),
            append_output(second_id, 3),
        ]]),
    );

    let batch = group
        .begin_proposal_batch(vec![
            Proposal {
                local_proposal_id: first_id,
                client_request_id: None,
                command: b"first".to_vec(),
            },
            Proposal {
                local_proposal_id: second_id,
                client_request_id: Some(ClientRequestId {
                    client_id: 12,
                    sequence: 1,
                }),
                command: b"second".to_vec(),
            },
        ])
        .expect("proposal batch starts");

    assert!(group.runtime().step_batches.is_empty());
    assert_eq!(group.runtime().proposal_batches.len(), 1);
    assert_eq!(
        group.runtime().proposal_batches[0],
        vec![
            ClientProposalInput {
                proposal_id: Some(first_id),
                payload: b"first".to_vec(),
            },
            ClientProposalInput {
                proposal_id: Some(second_id),
                payload: b"second".to_vec(),
            },
        ]
    );
    assert_eq!(
        batch.report.proposal_events,
        vec![
            ProposalEvent::Appended {
                local_proposal_id: first_id,
                index: LogIndex(2),
                term: Term(1),
            },
            ProposalEvent::Appended {
                local_proposal_id: second_id,
                index: LogIndex(3),
                term: Term(1),
            },
        ]
    );
    assert_eq!(
        batch
            .begins
            .iter()
            .map(|begin| match begin {
                ProposalBegin::Appended {
                    local_proposal_id,
                    index,
                    ..
                } => (*local_proposal_id, *index),
                other => panic!("expected appended begin, got {other:?}"),
            })
            .collect::<Vec<_>>(),
        vec![(first_id, LogIndex(2)), (second_id, LogIndex(3))]
    );
    assert_eq!(group.local_proposal_id_watermark(), Some(second_id));
    assert_eq!(group.metrics().pending_proposals, 2);
}

#[test]
fn group_step_proposal_batch_submits_one_runtime_batch_and_preserves_order() {
    let first_id = LocalProposalId(110);
    let second_id = LocalProposalId(111);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine::default(),
        ScriptedRuntime::with_step_outputs([vec![
            append_output(first_id, 2),
            append_output(second_id, 3),
        ]]),
    );

    let report = group
        .step(GroupInput::ProposalBatch {
            proposals: vec![
                Proposal {
                    local_proposal_id: first_id,
                    client_request_id: None,
                    command: b"first".to_vec(),
                },
                Proposal {
                    local_proposal_id: second_id,
                    client_request_id: Some(ClientRequestId {
                        client_id: 12,
                        sequence: 2,
                    }),
                    command: b"second".to_vec(),
                },
            ],
        })
        .expect("proposal batch starts through group input");

    assert!(group.runtime().step_batches.is_empty());
    assert_eq!(group.runtime().proposal_batches.len(), 1);
    assert_eq!(
        group.runtime().proposal_batches[0],
        vec![
            ClientProposalInput {
                proposal_id: Some(first_id),
                payload: b"first".to_vec(),
            },
            ClientProposalInput {
                proposal_id: Some(second_id),
                payload: b"second".to_vec(),
            },
        ]
    );
    assert_eq!(
        report.proposal_events,
        vec![
            ProposalEvent::Appended {
                local_proposal_id: first_id,
                index: LogIndex(2),
                term: Term(1),
            },
            ProposalEvent::Appended {
                local_proposal_id: second_id,
                index: LogIndex(3),
                term: Term(1),
            },
        ]
    );
    assert_eq!(group.local_proposal_id_watermark(), Some(second_id));
    assert_eq!(group.metrics().pending_proposals, 2);
}

#[test]
fn step_report_options_can_omit_metrics_without_changing_events() {
    let outputs = vec![
        append_output(LocalProposalId(120), 2),
        append_output(LocalProposalId(121), 3),
        apply_output(2, b"first", Some(LocalProposalId(120))),
        apply_output(3, b"second", Some(LocalProposalId(121))),
    ];
    let input = GroupInput::ProposalBatch {
        proposals: vec![
            Proposal {
                local_proposal_id: LocalProposalId(120),
                client_request_id: None,
                command: b"first".to_vec(),
            },
            Proposal {
                local_proposal_id: LocalProposalId(121),
                client_request_id: None,
                command: b"second".to_vec(),
            },
        ],
    };
    let mut full_group = scripted_group_with_runtime(
        RecordingStateMachine::default(),
        ScriptedRuntime::with_step_outputs([outputs.clone()]),
    );
    let mut lean_group = scripted_group_with_runtime(
        RecordingStateMachine::default(),
        ScriptedRuntime::with_step_outputs([outputs]),
    );

    let mut full = full_group.step(input.clone()).expect("full step succeeds");
    let lean = lean_group
        .step_with_options(input, StepReportOptions::without_metrics())
        .expect("lean step succeeds");

    assert!(full.metrics.is_some());
    assert!(lean.metrics.is_none());
    full.metrics = None;
    assert_eq!(lean, full);
}

#[test]
fn begin_proposal_batch_with_no_proposals_does_not_step_runtime() {
    let mut group = scripted_group(RecordingStateMachine::default());

    let batch = group
        .begin_proposal_batch(Vec::new())
        .expect("empty proposal batch is accepted");

    assert!(batch.begins.is_empty());
    assert!(batch.report.proposal_events.is_empty());
    assert!(group.runtime().step_batches.is_empty());
    assert!(group.runtime().proposal_batches.is_empty());
    assert_eq!(group.local_proposal_id_watermark(), None);
}

#[test]
fn begin_proposal_batch_rejects_non_monotonic_ids_before_runtime_submission() {
    let mut group = scripted_group(RecordingStateMachine::default());

    let error = group
        .begin_proposal_batch(vec![
            Proposal {
                local_proposal_id: LocalProposalId(12),
                client_request_id: None,
                command: b"first".to_vec(),
            },
            Proposal {
                local_proposal_id: LocalProposalId(11),
                client_request_id: None,
                command: b"lower".to_vec(),
            },
        ])
        .expect_err("non-monotonic batch is rejected");

    assert!(matches!(
        error,
        GroupError::NonMonotonicLocalProposalId {
            local_proposal_id: LocalProposalId(11),
            last_seen_local_proposal_id: LocalProposalId(12),
        }
    ));
    assert_eq!(group.local_proposal_id_watermark(), None);
    assert_eq!(group.metrics().pending_proposals, 0);
    assert!(group.runtime().step_batches.is_empty());
    assert!(group.runtime().proposal_batches.is_empty());
}

#[test]
fn runtime_batch_error_consumes_local_proposal_ids_and_clears_pending() {
    let first_id = LocalProposalId(102);
    let second_id = LocalProposalId(103);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine::default(),
        ScriptedRuntime::with_step_errors([TestRuntimeError::Forced]),
    );

    let error = group
        .begin_proposal_batch(vec![
            Proposal {
                local_proposal_id: first_id,
                client_request_id: None,
                command: b"first".to_vec(),
            },
            Proposal {
                local_proposal_id: second_id,
                client_request_id: None,
                command: b"second".to_vec(),
            },
        ])
        .expect_err("runtime batch error is returned");

    assert!(matches!(error, GroupError::Runtime(_)));
    assert_eq!(group.local_proposal_id_watermark(), Some(second_id));
    assert_eq!(group.metrics().pending_proposals, 0);
    assert_eq!(group.runtime().proposal_batches.len(), 1);
}

#[test]
fn begin_proposal_batch_clears_batch_pending_when_any_proposal_does_not_start() {
    let first_id = LocalProposalId(104);
    let second_id = LocalProposalId(105);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine::default(),
        ScriptedRuntime::with_step_outputs([vec![append_output(first_id, 2)]]),
    );

    let error = group
        .begin_proposal_batch(vec![
            Proposal {
                local_proposal_id: first_id,
                client_request_id: None,
                command: b"first".to_vec(),
            },
            Proposal {
                local_proposal_id: second_id,
                client_request_id: None,
                command: b"missing lifecycle".to_vec(),
            },
        ])
        .expect_err("missing one lifecycle output rejects the whole begin batch");

    assert!(matches!(
        error,
        GroupError::ProposalDidNotStart {
            local_proposal_id
        } if local_proposal_id == second_id
    ));
    assert_eq!(group.local_proposal_id_watermark(), Some(second_id));
    assert_eq!(group.metrics().pending_proposals, 0);
    assert_eq!(group.runtime().proposal_batches.len(), 1);
}

#[test]
fn begin_proposal_clears_pending_when_proposal_does_not_start() {
    let proposal_id = LocalProposalId(82);
    let mut group = scripted_group(RecordingStateMachine::default());

    let error = group
        .begin_proposal_outcome(Proposal {
            local_proposal_id: proposal_id,
            client_request_id: Some(ClientRequestId {
                client_id: 8,
                sequence: 20,
            }),
            command: b"no event".to_vec(),
        })
        .expect_err("no lifecycle event is rejected");

    assert!(matches!(
        error,
        GroupError::ProposalDidNotStart {
            local_proposal_id
        } if local_proposal_id == proposal_id
    ));
    assert_eq!(group.metrics().pending_proposals, 0);
    assert_eq!(group.local_proposal_id_watermark(), Some(proposal_id));
    assert_eq!(group.runtime().step_inputs.len(), 1);

    let reuse = group
        .begin_proposal_outcome(Proposal {
            local_proposal_id: proposal_id,
            client_request_id: None,
            command: b"reuse after did not start".to_vec(),
        })
        .expect_err("proposal-did-not-start local ID is consumed");
    assert!(matches!(
        reuse,
        GroupError::NonMonotonicLocalProposalId {
            local_proposal_id,
            last_seen_local_proposal_id,
        } if local_proposal_id == proposal_id
            && last_seen_local_proposal_id == proposal_id
    ));
}

#[test]
fn group_step_proposal_clears_pending_when_proposal_does_not_start() {
    let proposal_id = LocalProposalId(83);
    let mut group = scripted_group(RecordingStateMachine::default());

    let error = group
        .step(GroupInput::Proposal {
            proposal: Proposal {
                local_proposal_id: proposal_id,
                client_request_id: Some(ClientRequestId {
                    client_id: 8,
                    sequence: 21,
                }),
                command: b"no event".to_vec(),
            },
        })
        .expect_err("no lifecycle event is rejected");

    assert!(matches!(
        error,
        GroupError::ProposalDidNotStart {
            local_proposal_id
        } if local_proposal_id == proposal_id
    ));
    assert_eq!(group.metrics().pending_proposals, 0);
    assert_eq!(group.runtime().step_inputs.len(), 1);
}

#[test]
fn begin_proposal_surfaces_synchronous_unknown_outcome() {
    let proposal_id = LocalProposalId(84);
    let client_request_id = Some(ClientRequestId {
        client_id: 8,
        sequence: 22,
    });
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine::default(),
        ScriptedRuntime::with_step_outputs([vec![RaftOutput::LocalProposalDropped {
            proposal_id,
            index: LogIndex(2),
            term: Term(1),
            reason: rafter::LocalProposalDropReason::LeadershipLost,
        }]]),
    );

    let begin = group
        .begin_proposal_outcome(Proposal {
            local_proposal_id: proposal_id,
            client_request_id,
            command: b"unknown".to_vec(),
        })
        .expect("unknown outcome is a proposal lifecycle result");

    assert!(matches!(
        begin,
        ProposalBegin::UnknownOutcome {
            group_id: 7,
            local_proposal_id,
            client_request_id: returned_client_request_id,
            reason: ProposalUnknownOutcomeReason::LocalProposalDropped {
                index: LogIndex(2),
                term: Term(1),
                reason: rafter::LocalProposalDropReason::LeadershipLost,
            },
            peer_messages,
        } if local_proposal_id == proposal_id
            && returned_client_request_id == client_request_id
            && peer_messages.is_empty()
    ));
    assert_eq!(group.metrics().pending_proposals, 0);
}

#[test]
fn apply_batch_preserves_committed_order() {
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine::default(),
        ScriptedRuntime::with_step_outputs([
            vec![append_output(LocalProposalId(10), 2)],
            vec![append_output(LocalProposalId(11), 3)],
        ]),
    );
    begin_pending_proposal(&mut group, LocalProposalId(10), None, 2);
    begin_pending_proposal(&mut group, LocalProposalId(11), None, 3);

    let report = group
        .apply_raft_outputs(vec![
            apply_output(2, b"first", Some(LocalProposalId(10))),
            apply_output(3, b"second", Some(LocalProposalId(11))),
        ])
        .expect("batch applies");

    assert_eq!(
        group.state_machine().batches,
        vec![vec![LogIndex(2), LogIndex(3)]]
    );
    assert_eq!(
        group.state_machine().applied,
        vec![b"first".to_vec(), b"second".to_vec()]
    );
    assert_eq!(
        report
            .applied
            .iter()
            .map(|result| result.index)
            .collect::<Vec<_>>(),
        vec![LogIndex(2), LogIndex(3)]
    );
    assert_eq!(
        report
            .proposal_events
            .iter()
            .filter_map(|event| match event {
                ProposalEvent::Applied {
                    local_proposal_id, ..
                } => Some(*local_proposal_id),
                _ => None,
            })
            .collect::<Vec<_>>(),
        vec![LocalProposalId(10), LocalProposalId(11)]
    );
}

#[test]
fn dropped_local_proposal_becomes_unknown_outcome_and_clears_pending() {
    let proposal_id = LocalProposalId(70);
    let client_request_id = Some(ClientRequestId {
        client_id: 9,
        sequence: 3,
    });
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine::default(),
        ScriptedRuntime::with_step_outputs([vec![append_output(proposal_id, 2)]]),
    );
    begin_pending_proposal(&mut group, proposal_id, client_request_id, 2);

    let report = group
        .apply_raft_outputs(vec![RaftOutput::LocalProposalDropped {
            proposal_id,
            index: LogIndex(2),
            term: Term(1),
            reason: rafter::LocalProposalDropReason::LeadershipLost,
        }])
        .expect("drop output maps to proposal event");

    assert_eq!(
        report.proposal_events,
        vec![ProposalEvent::UnknownOutcome {
            local_proposal_id: proposal_id,
            client_request_id,
            reason: ProposalUnknownOutcomeReason::LocalProposalDropped {
                index: LogIndex(2),
                term: Term(1),
                reason: rafter::LocalProposalDropReason::LeadershipLost,
            },
        }]
    );
    assert_eq!(group.metrics().pending_proposals, 0);
}

#[test]
fn appended_local_proposal_event_requires_pending_proposal() {
    let proposal_id = LocalProposalId(80);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine::default(),
        ScriptedRuntime::with_step_outputs([vec![append_output(proposal_id, 2)]]),
    );
    begin_pending_proposal(&mut group, proposal_id, None, 2);

    let report = group
        .apply_raft_outputs(vec![append_output(proposal_id, 2)])
        .expect("pending append is reported");

    assert_eq!(
        report.proposal_events,
        vec![ProposalEvent::Appended {
            local_proposal_id: proposal_id,
            index: LogIndex(2),
            term: Term(1),
        }]
    );
    assert_eq!(group.metrics().pending_proposals, 1);
}

#[test]
fn stale_local_proposal_apply_after_apply_is_reported_without_lifecycle_event() {
    let proposal_id = LocalProposalId(77);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine::default(),
        ScriptedRuntime::with_step_outputs([vec![append_output(proposal_id, 2)]]),
    );
    begin_pending_proposal(&mut group, proposal_id, None, 2);

    let applied = group
        .apply_raft_outputs(vec![apply_output(2, b"original", Some(proposal_id))])
        .expect("proposal applies");
    assert_eq!(
        applied.proposal_events,
        vec![ProposalEvent::Applied {
            local_proposal_id: proposal_id,
            index: LogIndex(2),
            term: Term(1),
            result: b"original".to_vec(),
        }]
    );

    let stale = group
        .apply_raft_outputs(vec![apply_output(3, b"stale", Some(proposal_id))])
        .expect("stale apply result is still reported");

    assert!(stale.proposal_events.is_empty());
    assert_eq!(stale.applied.len(), 1);
    assert_eq!(stale.applied[0].local_proposal_id, Some(proposal_id));
    assert_eq!(stale.applied[0].result, b"stale".to_vec());
    assert_eq!(group.metrics().pending_proposals, 0);
}

#[test]
fn stale_local_proposal_append_after_apply_is_ignored() {
    let proposal_id = LocalProposalId(81);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine::default(),
        ScriptedRuntime::with_step_outputs([vec![append_output(proposal_id, 2)]]),
    );
    begin_pending_proposal(&mut group, proposal_id, None, 2);

    group
        .apply_raft_outputs(vec![apply_output(2, b"original", Some(proposal_id))])
        .expect("proposal applies");

    let stale = group
        .apply_raft_outputs(vec![append_output(proposal_id, 3)])
        .expect("stale append is ignored");

    assert!(stale.proposal_events.is_empty());
    assert_eq!(group.metrics().pending_proposals, 0);
}

#[test]
fn stale_local_proposal_apply_after_rejection_is_reported_without_lifecycle_event() {
    let proposal_id = LocalProposalId(78);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine::default(),
        ScriptedRuntime::with_step_outputs([vec![append_output(proposal_id, 2)]]),
    );
    begin_pending_proposal(&mut group, proposal_id, None, 2);

    group
        .apply_raft_outputs(vec![RaftOutput::RejectProposal {
            proposal_id: Some(proposal_id),
            reason: ProposalRejection::NotLeader {
                role: Role::Follower,
                term: Term(1),
                payload_len: 0,
            },
        }])
        .expect("proposal is rejected");

    let stale = group
        .apply_raft_outputs(vec![apply_output(2, b"stale", Some(proposal_id))])
        .expect("stale apply result is still reported");

    assert!(stale.proposal_events.is_empty());
    assert_eq!(stale.applied.len(), 1);
    assert_eq!(stale.applied[0].local_proposal_id, Some(proposal_id));
    assert_eq!(stale.applied[0].result, b"stale".to_vec());
    assert_eq!(group.metrics().pending_proposals, 0);
}

#[test]
fn stale_local_proposal_append_after_rejection_is_ignored() {
    let proposal_id = LocalProposalId(82);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine::default(),
        ScriptedRuntime::with_step_outputs([vec![append_output(proposal_id, 2)]]),
    );
    begin_pending_proposal(&mut group, proposal_id, None, 2);

    group
        .apply_raft_outputs(vec![RaftOutput::RejectProposal {
            proposal_id: Some(proposal_id),
            reason: ProposalRejection::NotLeader {
                role: Role::Follower,
                term: Term(1),
                payload_len: 0,
            },
        }])
        .expect("proposal is rejected");

    let stale = group
        .apply_raft_outputs(vec![append_output(proposal_id, 2)])
        .expect("stale append is ignored");

    assert!(stale.proposal_events.is_empty());
    assert_eq!(group.metrics().pending_proposals, 0);
}

#[test]
fn stale_local_proposal_apply_after_unknown_outcome_is_reported_without_lifecycle_event() {
    let proposal_id = LocalProposalId(79);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine::default(),
        ScriptedRuntime::with_step_outputs([vec![append_output(proposal_id, 2)]]),
    );
    begin_pending_proposal(&mut group, proposal_id, None, 2);

    group
        .apply_raft_outputs(vec![RaftOutput::LocalProposalDropped {
            proposal_id,
            index: LogIndex(2),
            term: Term(1),
            reason: rafter::LocalProposalDropReason::LeadershipLost,
        }])
        .expect("proposal becomes unknown outcome");

    let stale = group
        .apply_raft_outputs(vec![apply_output(2, b"stale", Some(proposal_id))])
        .expect("stale apply result is still reported");

    assert!(stale.proposal_events.is_empty());
    assert_eq!(stale.applied.len(), 1);
    assert_eq!(stale.applied[0].local_proposal_id, Some(proposal_id));
    assert_eq!(stale.applied[0].result, b"stale".to_vec());
    assert_eq!(group.metrics().pending_proposals, 0);
}

#[test]
fn stale_local_proposal_append_after_unknown_outcome_is_ignored() {
    let proposal_id = LocalProposalId(83);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine::default(),
        ScriptedRuntime::with_step_outputs([vec![append_output(proposal_id, 2)]]),
    );
    begin_pending_proposal(&mut group, proposal_id, None, 2);

    group
        .apply_raft_outputs(vec![RaftOutput::LocalProposalDropped {
            proposal_id,
            index: LogIndex(2),
            term: Term(1),
            reason: rafter::LocalProposalDropReason::LeadershipLost,
        }])
        .expect("proposal becomes unknown outcome");

    let stale = group
        .apply_raft_outputs(vec![append_output(proposal_id, 3)])
        .expect("stale append is ignored");

    assert!(stale.proposal_events.is_empty());
    assert_eq!(group.metrics().pending_proposals, 0);
}

#[test]
fn stale_lower_local_proposal_append_does_not_affect_higher_pending_proposal() {
    let stale_id = LocalProposalId(84);
    let higher_id = LocalProposalId(85);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine::default(),
        ScriptedRuntime::with_step_outputs([vec![append_output(higher_id, 3)]]),
    );
    begin_pending_proposal(&mut group, higher_id, None, 3);

    let stale = group
        .apply_raft_outputs(vec![append_output(stale_id, 2)])
        .expect("stale lower append is ignored");

    assert!(stale.proposal_events.is_empty());
    assert_eq!(group.metrics().pending_proposals, 1);
}

#[test]
fn stale_local_proposal_drop_after_apply_is_ignored() {
    let proposal_id = LocalProposalId(71);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine::default(),
        ScriptedRuntime::with_step_outputs([vec![append_output(proposal_id, 2)]]),
    );
    begin_pending_proposal(&mut group, proposal_id, None, 2);

    let applied = group
        .apply_raft_outputs(vec![apply_output(2, b"applied", Some(proposal_id))])
        .expect("proposal applies");
    assert_eq!(
        applied.proposal_events,
        vec![ProposalEvent::Applied {
            local_proposal_id: proposal_id,
            index: LogIndex(2),
            term: Term(1),
            result: b"applied".to_vec(),
        }]
    );
    assert_eq!(group.metrics().pending_proposals, 0);

    let stale_drop = group
        .apply_raft_outputs(vec![RaftOutput::LocalProposalDropped {
            proposal_id,
            index: LogIndex(2),
            term: Term(1),
            reason: rafter::LocalProposalDropReason::LeadershipLost,
        }])
        .expect("stale drop is ignored");

    assert!(stale_drop.proposal_events.is_empty());
    assert_eq!(group.metrics().pending_proposals, 0);
}

#[test]
fn stale_local_proposal_drop_after_rejection_is_ignored() {
    let proposal_id = LocalProposalId(72);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine::default(),
        ScriptedRuntime::with_step_outputs([vec![append_output(proposal_id, 2)]]),
    );
    begin_pending_proposal(&mut group, proposal_id, None, 2);

    let rejected = group
        .apply_raft_outputs(vec![RaftOutput::RejectProposal {
            proposal_id: Some(proposal_id),
            reason: ProposalRejection::NotLeader {
                role: Role::Follower,
                term: Term(1),
                payload_len: 0,
            },
        }])
        .expect("proposal rejection is reported");
    assert_eq!(
        rejected.proposal_events,
        vec![ProposalEvent::Rejected {
            local_proposal_id: proposal_id,
            reason: ProposalRejection::NotLeader {
                role: Role::Follower,
                term: Term(1),
                payload_len: 0,
            },
            leader_hint: Some(NodeId(1)),
        }]
    );
    assert_eq!(group.metrics().pending_proposals, 0);

    let stale_drop = group
        .apply_raft_outputs(vec![RaftOutput::LocalProposalDropped {
            proposal_id,
            index: LogIndex(3),
            term: Term(1),
            reason: rafter::LocalProposalDropReason::LeadershipLost,
        }])
        .expect("stale drop is ignored");

    assert!(stale_drop.proposal_events.is_empty());
    assert_eq!(group.metrics().pending_proposals, 0);
}

#[test]
fn reused_local_proposal_id_is_rejected_and_stale_drop_is_ignored() {
    let proposal_id = LocalProposalId(73);
    let higher_id = LocalProposalId(75);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine::default(),
        ScriptedRuntime::with_step_outputs([
            vec![RaftOutput::LocalProposalAppended {
                proposal_id,
                index: LogIndex(2),
                term: Term(1),
            }],
            vec![RaftOutput::LocalProposalAppended {
                proposal_id: higher_id,
                index: LogIndex(3),
                term: Term(1),
            }],
        ]),
    );

    let begin = group
        .begin_proposal_outcome(Proposal {
            local_proposal_id: proposal_id,
            client_request_id: None,
            command: b"first".to_vec(),
        })
        .expect("first proposal starts");
    assert!(matches!(
        begin,
        ProposalBegin::Appended {
            local_proposal_id,
            ..
        } if local_proposal_id == proposal_id
    ));
    group
        .apply_raft_outputs(vec![apply_output(2, b"first", Some(proposal_id))])
        .expect("first proposal applies");

    let reuse_error = group
        .begin_proposal_outcome(Proposal {
            local_proposal_id: proposal_id,
            client_request_id: None,
            command: b"reuse".to_vec(),
        })
        .expect_err("local proposal IDs are single-use");
    assert!(matches!(
        reuse_error,
        GroupError::NonMonotonicLocalProposalId {
            local_proposal_id,
            last_seen_local_proposal_id,
        } if local_proposal_id == proposal_id
            && last_seen_local_proposal_id == proposal_id
    ));
    assert_eq!(group.metrics().pending_proposals, 0);

    let higher = group
        .begin_proposal_outcome(Proposal {
            local_proposal_id: higher_id,
            client_request_id: None,
            command: b"higher".to_vec(),
        })
        .expect("higher fresh local proposal ID is accepted");
    assert!(matches!(
        higher,
        ProposalBegin::Appended {
            local_proposal_id,
            index: LogIndex(3),
            ..
        } if local_proposal_id == higher_id
    ));
    assert_eq!(group.metrics().pending_proposals, 1);

    let stale_drop = group
        .apply_raft_outputs(vec![RaftOutput::LocalProposalDropped {
            proposal_id,
            index: LogIndex(2),
            term: Term(1),
            reason: rafter::LocalProposalDropReason::LeadershipLost,
        }])
        .expect("stale drop is ignored after rejected reuse");

    assert!(stale_drop.proposal_events.is_empty());
    assert_eq!(group.metrics().pending_proposals, 1);
}

#[test]
fn reused_local_proposal_id_is_rejected_and_stale_rejection_is_ignored() {
    let proposal_id = LocalProposalId(74);
    let higher_id = LocalProposalId(76);
    let rejection = ProposalRejection::NotLeader {
        role: Role::Follower,
        term: Term(1),
        payload_len: 0,
    };
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine::default(),
        ScriptedRuntime::with_step_outputs([
            vec![RaftOutput::RejectProposal {
                proposal_id: Some(proposal_id),
                reason: rejection.clone(),
            }],
            vec![RaftOutput::LocalProposalAppended {
                proposal_id: higher_id,
                index: LogIndex(3),
                term: Term(1),
            }],
        ]),
    );

    let begin = group
        .begin_proposal_outcome(Proposal {
            local_proposal_id: proposal_id,
            client_request_id: None,
            command: b"first".to_vec(),
        })
        .expect("first proposal is rejected");
    assert!(matches!(
        begin,
        ProposalBegin::Rejected {
            local_proposal_id,
            reason,
            ..
        } if local_proposal_id == proposal_id && reason == rejection
    ));

    let reuse_error = group
        .step(GroupInput::Proposal {
            proposal: Proposal {
                local_proposal_id: proposal_id,
                client_request_id: None,
                command: b"reuse".to_vec(),
            },
        })
        .expect_err("local proposal IDs are single-use");
    assert!(matches!(
        reuse_error,
        GroupError::NonMonotonicLocalProposalId {
            local_proposal_id,
            last_seen_local_proposal_id,
        } if local_proposal_id == proposal_id
            && last_seen_local_proposal_id == proposal_id
    ));
    assert_eq!(group.metrics().pending_proposals, 0);

    let higher = group
        .begin_proposal_outcome(Proposal {
            local_proposal_id: higher_id,
            client_request_id: None,
            command: b"higher".to_vec(),
        })
        .expect("higher fresh local proposal ID is accepted");
    assert!(matches!(
        higher,
        ProposalBegin::Appended {
            local_proposal_id,
            index: LogIndex(3),
            ..
        } if local_proposal_id == higher_id
    ));
    assert_eq!(group.metrics().pending_proposals, 1);

    let stale_rejection = group
        .apply_raft_outputs(vec![RaftOutput::RejectProposal {
            proposal_id: Some(proposal_id),
            reason: rejection,
        }])
        .expect("stale rejection is ignored after rejected reuse");

    assert!(stale_rejection.proposal_events.is_empty());
    assert_eq!(group.metrics().pending_proposals, 1);
}

/// A follower that knows its leader hands the redirect to a caller who observes
/// the rejection asynchronously, not only to one that reads the begin outcome.
#[test]
fn rejected_proposal_event_carries_the_leader_hint() {
    let proposal_id = LocalProposalId(101);
    let mut runtime = ScriptedRuntime::with_step_outputs([
        vec![RaftOutput::LocalProposalAppended {
            proposal_id,
            index: LogIndex(2),
            term: Term(1),
        }],
        Vec::new(),
    ]);
    runtime.role = Role::Follower;
    runtime.leader_hint = Some(NodeId(3));
    let mut group = scripted_group_with_runtime(RecordingStateMachine::default(), runtime);
    begin_pending_proposal(&mut group, proposal_id, None, 2);

    let report = group
        .apply_raft_outputs(vec![RaftOutput::RejectProposal {
            proposal_id: Some(proposal_id),
            reason: ProposalRejection::NotLeader {
                role: Role::Follower,
                term: Term(1),
                payload_len: 0,
            },
        }])
        .expect("proposal rejection is reported");

    assert_eq!(
        report.proposal_events,
        vec![ProposalEvent::Rejected {
            local_proposal_id: proposal_id,
            reason: ProposalRejection::NotLeader {
                role: Role::Follower,
                term: Term(1),
                payload_len: 0,
            },
            leader_hint: Some(NodeId(3)),
        }]
    );
}

/// `None` means "no leader is known", which a caller must treat as "retry
/// discovery" rather than "not applicable".
#[test]
fn rejected_proposal_event_carries_no_hint_when_no_leader_is_known() {
    let proposal_id = LocalProposalId(102);
    let mut runtime = ScriptedRuntime::with_step_outputs([
        vec![RaftOutput::LocalProposalAppended {
            proposal_id,
            index: LogIndex(2),
            term: Term(1),
        }],
        Vec::new(),
    ]);
    runtime.role = Role::Follower;
    runtime.leader_hint = None;
    let mut group = scripted_group_with_runtime(RecordingStateMachine::default(), runtime);
    begin_pending_proposal(&mut group, proposal_id, None, 2);

    let report = group
        .apply_raft_outputs(vec![RaftOutput::RejectProposal {
            proposal_id: Some(proposal_id),
            reason: ProposalRejection::NotLeader {
                role: Role::Follower,
                term: Term(1),
                payload_len: 0,
            },
        }])
        .expect("proposal rejection is reported");

    assert!(matches!(
        report.proposal_events.as_slice(),
        [ProposalEvent::Rejected {
            leader_hint: None,
            ..
        }]
    ));
}

/// The immediate and asynchronous views of one rejection read the same hint,
/// because the begin outcome now takes it from the event instead of re-reading
/// the runtime after the whole report is built.
#[test]
fn rejected_proposal_event_hint_matches_the_immediate_begin_outcome() {
    let proposal_id = LocalProposalId(103);
    let mut runtime = ScriptedRuntime::with_step_outputs([vec![RaftOutput::RejectProposal {
        proposal_id: Some(proposal_id),
        reason: ProposalRejection::NotLeader {
            role: Role::Follower,
            term: Term(1),
            payload_len: 0,
        },
    }]]);
    runtime.role = Role::Follower;
    runtime.leader_hint = Some(NodeId(2));
    let mut group = scripted_group_with_runtime(RecordingStateMachine::default(), runtime);

    let started = group
        .begin_proposal(Proposal {
            local_proposal_id: proposal_id,
            client_request_id: None,
            command: b"redirect me".to_vec(),
        })
        .expect("a rejected proposal still reports a begin outcome");

    let event_hint = started
        .report
        .proposal_events
        .iter()
        .find_map(|event| match event {
            ProposalEvent::Rejected { leader_hint, .. } => Some(*leader_hint),
            _ => None,
        })
        .expect("the report carries the rejection");
    let begin_hint = match started.begin {
        ProposalBegin::Rejected { leader_hint, .. } => leader_hint,
        other => panic!("expected a rejected begin outcome, got {other:?}"),
    };
    assert_eq!(event_hint, Some(NodeId(2)));
    assert_eq!(begin_hint, event_hint);
}

/// The hint is this node's belief about leadership, not a claim about why the
/// proposal was refused — a caller must not read a present hint as "this was a
/// leadership rejection".
#[test]
fn a_non_leadership_rejection_still_reports_the_current_hint() {
    let proposal_id = LocalProposalId(104);
    let mut runtime = ScriptedRuntime::with_step_outputs([
        vec![RaftOutput::LocalProposalAppended {
            proposal_id,
            index: LogIndex(2),
            term: Term(1),
        }],
        Vec::new(),
    ]);
    runtime.leader_hint = Some(NodeId(1));
    let mut group = scripted_group_with_runtime(RecordingStateMachine::default(), runtime);
    begin_pending_proposal(&mut group, proposal_id, None, 2);

    let report = group
        .apply_raft_outputs(vec![RaftOutput::RejectProposal {
            proposal_id: Some(proposal_id),
            reason: ProposalRejection::PayloadTooLarge {
                payload_len: 4096,
                max_payload_len: 1024,
            },
        }])
        .expect("payload rejection is reported");

    assert_eq!(
        report.proposal_events,
        vec![ProposalEvent::Rejected {
            local_proposal_id: proposal_id,
            reason: ProposalRejection::PayloadTooLarge {
                payload_len: 4096,
                max_payload_len: 1024,
            },
            // This node is the leader and still refused the write.
            leader_hint: Some(NodeId(1)),
        }]
    );
}
