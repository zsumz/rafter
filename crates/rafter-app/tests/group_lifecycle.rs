#![allow(clippy::wildcard_imports)]

mod support;

use support::*;

#[test]
fn single_node_begin_proposal_completes_immediately() {
    let mut group = group(1, &[]);
    let report = group.step(GroupInput::Tick).expect("single node elects");
    assert!(report.peer_messages.is_empty());
    assert!(report.snapshot_events.is_empty());
    assert!(report.membership_events.is_empty());
    assert_eq!(
        report
            .metrics
            .as_ref()
            .expect("step report has metrics")
            .role,
        Role::Leader
    );

    let proposal_id = LocalProposalId(1);
    let begin = group
        .begin_proposal_outcome(Proposal {
            local_proposal_id: proposal_id,
            client_request_id: None,
            command: b"one".to_vec(),
        })
        .expect("single node proposal completes");

    assert!(matches!(
        begin,
        ProposalBegin::Completed {
            local_proposal_id,
            result,
            peer_messages,
            ..
        } if local_proposal_id == proposal_id
            && result == b"one".to_vec()
            && peer_messages.is_empty()
    ));
    assert_eq!(group.state_machine().applied, vec![b"one".to_vec()]);
    let metrics = group.metrics();
    assert_eq!(metrics.role, Role::Leader);
    assert_eq!(metrics.applied_index, LogIndex(2));
    assert_eq!(metrics.membership, membership(&[1], &[]));
    assert!(metrics.replication.is_empty());
}

#[test]
fn begin_proposal_preserves_coemitted_report_streams() {
    let proposal_id = LocalProposalId(81);
    let snapshot = test_snapshot(12);
    let staged = staged_snapshot_chunk(&snapshot);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine::default(),
        ScriptedRuntime::with_step_outputs([vec![
            append_output(proposal_id, 2),
            apply_output(3, b"side-effect", None),
            RaftOutput::StageSnapshotChunk {
                chunk: staged.clone(),
            },
            RaftOutput::LeadershipTransferRejected {
                target: NodeId(2),
                reason: LeadershipTransferRejection::TargetIsSelf,
            },
        ]]),
    );

    let full = group
        .begin_proposal(Proposal {
            local_proposal_id: proposal_id,
            client_request_id: None,
            command: b"proposal".to_vec(),
        })
        .expect("proposal begins with full report");

    assert!(matches!(
        full.begin,
        ProposalBegin::Appended {
            local_proposal_id,
            peer_messages,
            ..
        } if local_proposal_id == proposal_id && peer_messages.is_empty()
    ));
    assert_eq!(full.report.applied.len(), 1);
    assert_eq!(full.report.applied[0].index, LogIndex(3));
    assert_eq!(
        full.report.snapshot_events,
        vec![SnapshotEvent::StageChunk {
            group_id: 7,
            chunk: staged,
        }]
    );
    assert_eq!(
        full.report.leadership_transfer_events,
        vec![LeadershipTransferEvent::Rejected {
            target: NodeId(2),
            reason: LeadershipTransferRejection::TargetIsSelf,
            leader_hint: Some(NodeId(1)),
        }]
    );
    assert!(full.report.metrics.is_some());
}

#[test]
fn follower_metrics_cover_protocol_and_app_fields() {
    let group = group(1, &[2, 3]);

    let metrics = group.metrics();
    assert_eq!(metrics.group_id, 7);
    assert_eq!(metrics.node_id, NodeId(1));
    assert_eq!(metrics.role, Role::Follower);
    assert_eq!(metrics.term, Term(0));
    assert_eq!(metrics.leader_hint, None);
    assert_eq!(metrics.commit_index, LogIndex::ZERO);
    assert_eq!(metrics.applied_index, LogIndex::ZERO);
    assert_eq!(metrics.last_log_index, LogIndex::ZERO);
    assert_eq!(metrics.snapshot_index, LogIndex::ZERO);
    assert_eq!(metrics.membership, membership(&[1, 2, 3], &[]));
    assert!(metrics.replication.is_empty());
    assert_eq!(metrics.pending_proposals, 0);
    assert_eq!(metrics.pending_reads, 0);
    assert_eq!(metrics.fatal_state, GroupFatalState::Healthy);
}

#[test]
fn membership_add_learner_reports_applied_membership() {
    let mut group = group(1, &[]);
    group.step(GroupInput::Tick).expect("single node elects");

    let report = group
        .step(GroupInput::Membership {
            change: MembershipChange::AddLearner {
                node_id: NodeId(2),
                info: NodeInfo::default(),
            },
        })
        .expect("single node membership change commits");

    assert_eq!(report.membership_events.len(), 1);
    assert!(matches!(
        &report.membership_events[0],
        MembershipEvent::Applied {
            group_id: 7,
            index,
            term,
            membership,
        } if *index > LogIndex::ZERO
            && !term.is_zero()
            && membership.contains_voter(NodeId(1))
            && membership.contains_learner(NodeId(2))
    ));
    assert_eq!(group.metrics().membership, membership(&[1], &[2]));
}

#[test]
fn membership_request_rejection_reports_reason_and_leader_hint() {
    let mut group = group(1, &[2, 3]);

    let report = group
        .step(GroupInput::Membership {
            change: MembershipChange::AddLearner {
                node_id: NodeId(4),
                info: NodeInfo::default(),
            },
        })
        .expect("membership rejection is reported as a step result");

    assert_eq!(report.membership_events.len(), 1);
    assert!(matches!(
        &report.membership_events[0],
        MembershipEvent::Rejected {
            group_id: 7,
            reason: ProposalRejection::NotLeader {
                role: Role::Follower,
                term: Term(0),
                ..
            },
            leader_hint: None,
        }
    ));
    assert_eq!(group.metrics().membership, membership(&[1, 2, 3], &[]));
}

#[test]
fn membership_request_reports_uncommitted_append_event() {
    let mut group = scripted_group(RecordingStateMachine::default());

    let report = group
        .step(GroupInput::Membership {
            change: MembershipChange::AddLearner {
                node_id: NodeId(2),
                info: NodeInfo::default(),
            },
        })
        .expect("scripted runtime accepts membership request");

    assert_eq!(report.membership_events.len(), 1);
    assert!(matches!(
        &report.membership_events[0],
        MembershipEvent::Appended {
            group_id: 7,
            index,
            term: Term(1),
            membership,
        } if *index == LogIndex(1)
            && membership.contains_voter(NodeId(1))
            && membership.contains_learner(NodeId(2))
    ));
}

#[test]
fn transfer_rejection_reports_reason_and_leader_hint() {
    let mut group = RaftGroup::new(
        7,
        NodeId(1),
        ScriptedRuntime::with_step_outputs([vec![RaftOutput::LeadershipTransferRejected {
            target: NodeId(1),
            reason: LeadershipTransferRejection::TargetIsSelf,
        }]]),
        RecordingStateMachine::default(),
    );

    let report = group
        .step(GroupInput::TransferLeadership { target: NodeId(1) })
        .expect("transfer rejection is reported as a step result");

    assert_eq!(report.leadership_transfer_events.len(), 1);
    assert_eq!(
        report.leadership_transfer_events[0],
        LeadershipTransferEvent::Rejected {
            target: NodeId(1),
            reason: LeadershipTransferRejection::TargetIsSelf,
            leader_hint: Some(NodeId(1)),
        }
    );
}

#[test]
fn transfer_from_non_leader_reports_rejection() {
    let mut runtime =
        ScriptedRuntime::with_step_outputs([vec![RaftOutput::LeadershipTransferRejected {
            target: NodeId(2),
            reason: LeadershipTransferRejection::NotLeader,
        }]]);
    runtime.role = Role::Follower;
    runtime.leader_hint = Some(NodeId(3));
    let mut group = RaftGroup::new(7, NodeId(1), runtime, RecordingStateMachine::default());

    let report = group
        .step(GroupInput::TransferLeadership { target: NodeId(2) })
        .expect("non-leader transfer rejection is reported");

    assert_eq!(
        report.leadership_transfer_events,
        vec![LeadershipTransferEvent::Rejected {
            target: NodeId(2),
            reason: LeadershipTransferRejection::NotLeader,
            leader_hint: Some(NodeId(3)),
        }]
    );
}

#[test]
fn accepted_transfer_reports_started_event() {
    let mut group = scripted_group(RecordingStateMachine::default());

    let report = group
        .step(GroupInput::TransferLeadership { target: NodeId(2) })
        .expect("scripted runtime accepts transfer input");

    assert_eq!(
        report.leadership_transfer_events,
        vec![LeadershipTransferEvent::Started { target: NodeId(2) }]
    );
}

#[test]
fn membership_inputs_translate_to_core_membership_inputs() {
    let target = membership_set(&[1, 3, 4], &[2]);
    let barrier = PromotionBarrier {
        learner_id: NodeId(2),
        required_match_index: LogIndex(9),
    };
    let changes = [
        MembershipChange::AddLearner {
            node_id: NodeId(2),
            info: NodeInfo::default(),
        },
        MembershipChange::PromoteLearner {
            node_id: NodeId(2),
            barrier,
        },
        MembershipChange::RemoveNode { node_id: NodeId(3) },
        MembershipChange::EnterJoint {
            target: target.clone(),
            promotion_barriers: vec![barrier],
        },
        MembershipChange::LeaveJoint,
        MembershipChange::ChangeVoters {
            target: target.clone(),
        },
    ];
    let mut group = scripted_group(RecordingStateMachine::default());

    for change in changes {
        group
            .step(GroupInput::Membership { change })
            .expect("scripted runtime accepts membership input");
    }

    assert!(matches!(
        group.runtime().step_inputs[0],
        RaftInput::AddLearner {
            learner_id: NodeId(2)
        }
    ));
    assert!(matches!(
        group.runtime().step_inputs[1],
        RaftInput::PromoteLearner {
            learner_id: NodeId(2),
            promotion_barrier,
        } if promotion_barrier == barrier
    ));
    assert!(matches!(
        group.runtime().step_inputs[2],
        RaftInput::RemoveVoter {
            voter_id: NodeId(3)
        }
    ));
    assert!(matches!(
        &group.runtime().step_inputs[3],
        RaftInput::EnterJoint {
            target: input_target,
            promotion_barriers,
        } if *input_target == target && promotion_barriers.as_slice() == [barrier]
    ));
    assert!(matches!(
        group.runtime().step_inputs[4],
        RaftInput::LeaveJoint
    ));
    assert!(matches!(
        &group.runtime().step_inputs[5],
        RaftInput::ChangeMembership {
            target: input_target,
            promotion_barriers,
        } if *input_target == target && promotion_barriers.is_empty()
    ));
}

/// Decomposition hands back the two owned things a replacement incarnation
/// needs, without cloning either.
#[test]
fn into_parts_returns_the_state_machine_and_runtime_it_was_built_with() {
    let mut group = group(1, &[]);
    let _ = group.step(GroupInput::Tick).expect("single node elects");
    let _ = group
        .begin_proposal_outcome(Proposal {
            local_proposal_id: LocalProposalId(1),
            client_request_id: None,
            command: b"one".to_vec(),
        })
        .expect("single node proposal completes");

    let parts = group.into_parts();

    assert_eq!(parts.group_id, 7);
    assert_eq!(parts.node_id, NodeId(1));
    assert_eq!(parts.state_machine.applied, vec![b"one".to_vec()]);
    assert_eq!(parts.state_machine.applied_index, LogIndex(2));
    assert_eq!(parts.runtime.commit_index(), LogIndex(2));
    assert!(matches!(parts.fatal_state, GroupFatalState::Healthy));
    assert!(parts.poisoned_waiters.is_empty());
}

/// Poison is the state a caller most needs to leave, so decomposition works
/// there and carries the poison out rather than dropping it — a caller that
/// never called `drain_poisoned_waiters` can still resolve its clients.
#[test]
fn into_parts_reports_poison_and_the_waiters_it_drained() {
    let proposal_id = LocalProposalId(30);
    let read_id = ReadId(30);
    let client_request_id = Some(ClientRequestId {
        client_id: 4,
        sequence: 2,
    });
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine {
            apply_mode: ApplyMode::Fail,
            ..RecordingStateMachine::default()
        },
        ScriptedRuntime::with_step_outputs([vec![append_output(proposal_id, 2)], Vec::new()]),
    );
    begin_pending_proposal(&mut group, proposal_id, client_request_id, 2);
    begin_pending_read_barrier(&mut group, read_id, None);
    let _ = group
        .apply_raft_outputs(vec![apply_output(2, b"bad", Some(proposal_id))])
        .expect_err("apply failure poisons the group");
    assert!(!group.poisoned_waiters().is_empty());

    let parts = group.into_parts();

    assert!(matches!(
        parts.fatal_state,
        GroupFatalState::Poisoned { .. }
    ));
    assert_eq!(
        parts.poisoned_waiters.proposals,
        vec![(proposal_id, client_request_id)]
    );
    assert_eq!(parts.poisoned_waiters.reads, vec![read_id]);
}

/// The watermarks are the group's own consumed-ID floors, and they are what a
/// caller carrying the runtime forward has to allocate above.
#[test]
fn into_parts_reports_id_watermarks_after_use() {
    let proposal_id = LocalProposalId(41);
    let read_id = ReadId(42);
    let mut group = scripted_group_with_runtime(
        RecordingStateMachine::default(),
        ScriptedRuntime::with_step_outputs([vec![append_output(proposal_id, 2)], Vec::new()]),
    );
    let fresh = scripted_group(RecordingStateMachine::default()).into_parts();
    assert_eq!(fresh.local_proposal_id_watermark, None);
    assert_eq!(fresh.read_id_watermark, None);

    begin_pending_proposal(&mut group, proposal_id, None, 2);
    begin_pending_read_barrier(&mut group, read_id, None);

    let parts = group.into_parts();

    assert_eq!(parts.local_proposal_id_watermark, Some(proposal_id));
    assert_eq!(parts.read_id_watermark, Some(read_id));
}

/// The hazard the watermarks exist for, made executable.
///
/// A live runtime keeps tracking local proposal IDs for entries it has not
/// committed, and a group built over it starts with no watermark of its own. A
/// caller that restarts IDs at zero therefore gets its new waiter completed by
/// the *old* proposal's entry — right ID, wrong index, wrong result — and the
/// new proposal's own apply then finds no waiter left to report to. Nothing
/// here is a bug in either layer; it is why the watermarks are in the parts.
#[test]
fn a_group_rebuilt_over_the_same_runtime_completes_a_reused_id_with_the_older_result() {
    let reused_id = LocalProposalId(1);
    let mut group = group(1, &[2, 3]);
    let term = elect_group_leader(&mut group);
    let started = group
        .begin_proposal(Proposal {
            local_proposal_id: reused_id,
            client_request_id: None,
            command: b"old".to_vec(),
        })
        .expect("the leader appends the first proposal");
    assert!(matches!(
        started.begin,
        ProposalBegin::Appended {
            index: LogIndex(2),
            ..
        }
    ));

    let parts = group.into_parts();
    assert_eq!(parts.local_proposal_id_watermark, Some(reused_id));

    // The replacement group carries the live runtime and restarts its IDs at
    // zero, which is only safe over a runtime rebuilt from durable storage.
    let mut rebuilt = RaftGroup::new(
        parts.group_id,
        parts.node_id,
        parts.runtime,
        RecordingStateMachine::default(),
    );
    let started = rebuilt
        .begin_proposal(Proposal {
            local_proposal_id: reused_id,
            client_request_id: None,
            command: b"new".to_vec(),
        })
        .expect("the rebuilt group appends its own proposal");
    let peer_messages = match started.begin {
        ProposalBegin::Appended {
            index: LogIndex(3),
            peer_messages,
            ..
        } => peer_messages,
        other => panic!("expected the new proposal to append at index 3, got {other:?}"),
    };

    let report = acknowledge_replication(&mut rebuilt, term, LogIndex(3), &peer_messages);

    // The new waiter is completed by the retired incarnation's entry.
    assert_eq!(
        report.proposal_events,
        vec![ProposalEvent::Applied {
            local_proposal_id: reused_id,
            index: LogIndex(2),
            term,
            result: b"old".to_vec(),
        }],
        "a reused local proposal ID is completed by the older proposal's result"
    );
    // ...and the proposal the caller actually made reports nothing at all.
    assert!(report
        .applied
        .iter()
        .any(|result| result.index == LogIndex(3) && result.result == b"new"));
    assert_eq!(rebuilt.metrics().pending_proposals, 0);

    // Allocating strictly above the returned watermark avoids all of it.
    assert!(reused_id <= parts.local_proposal_id_watermark.expect("watermark is set"));
}
