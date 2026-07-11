use rafter::{Message, NodeId};

use super::super::helpers::{config, proposal_payload, request_vote};
use super::super::{
    replay_raft_trace, summarize, Action, MessageKind, ProposalId, ReplayCheck, ReplayExpectation,
};
use crate::Cluster;

#[test]
fn replay_raft_trace_reaches_expected_final_state() {
    let configs = vec![
        config(1, &[2, 3], 2),
        config(2, &[1, 3], 2),
        config(3, &[1, 2], 2),
    ];
    let trace = vec![
        Action::Tick(NodeId(1)),
        Action::Tick(NodeId(1)),
        Action::Deliver {
            from: NodeId(1),
            to: NodeId(2),
            message: MessageKind::RequestVote,
        },
    ];

    let mut expected_cluster = Cluster::new(configs.clone());
    expected_cluster.tick(NodeId(1));
    expected_cluster.tick(NodeId(1));
    assert!(expected_cluster.deliver_one_matching(request_vote(NodeId(1), NodeId(2))));
    let expected = summarize(&expected_cluster);

    let report = replay_raft_trace(
        configs,
        &trace,
        ReplayCheck::ElectionSafety,
        ReplayExpectation::FinalState(&expected),
    )
    .expect("trace replay should reach the expected final state");

    assert_eq!(report.state(), &expected);
    assert!(report.failure().is_none());
    assert_eq!(trace[2].to_string(), "deliver request_vote node-1->node-2");
}

#[test]
fn commit_safety_allows_old_leader_commit_before_newer_candidate_wins() {
    let configs = vec![
        config(1, &[2, 3], 2),
        config(2, &[1, 3], 2),
        config(3, &[1, 2], 2),
    ];
    let trace = vec![
        Action::Tick(NodeId(1)),
        Action::Tick(NodeId(1)),
        Action::Deliver {
            from: NodeId(1),
            to: NodeId(2),
            message: MessageKind::RequestVote,
        },
        Action::Tick(NodeId(2)),
        Action::Tick(NodeId(2)),
        Action::Deliver {
            from: NodeId(2),
            to: NodeId(1),
            message: MessageKind::RequestVoteResponse,
        },
        Action::Propose {
            to: NodeId(1),
            proposal_id: ProposalId(1),
        },
        Action::Deliver {
            from: NodeId(1),
            to: NodeId(3),
            message: MessageKind::AppendEntries,
        },
        Action::Deliver {
            from: NodeId(3),
            to: NodeId(1),
            message: MessageKind::AppendEntriesResponse,
        },
    ];

    let mut expected_cluster = Cluster::new(configs.clone());
    expected_cluster.tick(NodeId(1));
    expected_cluster.tick(NodeId(1));
    assert!(expected_cluster.deliver_one_matching(request_vote(NodeId(1), NodeId(2))));
    expected_cluster.tick(NodeId(2));
    expected_cluster.tick(NodeId(2));
    assert!(expected_cluster.deliver_one_matching(|envelope| {
        envelope.from == NodeId(2)
            && envelope.to == NodeId(1)
            && matches!(envelope.message, Message::RequestVoteResponse(_))
    }));
    expected_cluster.propose(NodeId(1), proposal_payload(ProposalId(1)));
    assert!(expected_cluster.deliver_one_matching(|envelope| {
        envelope.from == NodeId(1)
            && envelope.to == NodeId(3)
            && matches!(envelope.message, Message::AppendEntries(_))
    }));
    assert!(expected_cluster.deliver_one_matching(|envelope| {
        envelope.from == NodeId(3)
            && envelope.to == NodeId(1)
            && matches!(envelope.message, Message::AppendEntriesResponse(_))
    }));
    let expected = summarize(&expected_cluster);

    let report = replay_raft_trace(
        configs,
        &trace,
        ReplayCheck::CommitSafety,
        ReplayExpectation::FinalState(&expected),
    )
    .expect("a higher-term candidate is not yet a newer-term winning leader");

    assert_eq!(report.state(), &expected);
    assert!(report.failure().is_none());
}
