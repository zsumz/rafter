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
