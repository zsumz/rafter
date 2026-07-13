use rafter::{Message, NodeId};

use super::super::helpers::{config, proposal_payload, request_vote};
use super::super::{
    replay_raft_trace,
    scheduling::{deliver_action, Operation},
    state::{apply_to_state, ExplorationState},
    summarize, Action, EnvelopeIdentity, MessageKind, ProposalId, ReplayCheck, ReplayExpectation,
};
use crate::{Cluster, SimTick};

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
            identity: EnvelopeIdentity::new(SimTick(0), 0),
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
    assert_eq!(
        trace[2].to_string(),
        "deliver request_vote node-1->node-2 [ready=0, ordinal=0]"
    );
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
            identity: EnvelopeIdentity::new(SimTick(0), 0),
        },
        Action::Tick(NodeId(2)),
        Action::Tick(NodeId(2)),
        Action::Deliver {
            from: NodeId(2),
            to: NodeId(1),
            message: MessageKind::RequestVoteResponse,
            identity: EnvelopeIdentity::new(SimTick(0), 0),
        },
        Action::Propose {
            to: NodeId(1),
            proposal_id: ProposalId(1),
        },
        Action::Deliver {
            from: NodeId(1),
            to: NodeId(3),
            message: MessageKind::AppendEntries,
            identity: EnvelopeIdentity::new(SimTick(0), 0),
        },
        Action::Deliver {
            from: NodeId(3),
            to: NodeId(1),
            message: MessageKind::AppendEntriesResponse,
            identity: EnvelopeIdentity::new(SimTick(0), 0),
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

#[test]
fn replay_delivers_the_selected_envelope_when_routing_and_kind_collide() {
    let configs = vec![
        config(1, &[2, 3], 2),
        config(2, &[1, 3], 2),
        config(3, &[1, 2], 2),
    ];
    let mut state = ExplorationState::new(Cluster::new(configs.clone()));
    let mut trace = Vec::new();

    apply_and_record(
        &mut state,
        &mut trace,
        Action::Tick(NodeId(1)),
        Operation::Tick(NodeId(1)),
    );
    apply_and_record(
        &mut state,
        &mut trace,
        Action::Tick(NodeId(1)),
        Operation::Tick(NodeId(1)),
    );
    record_delivery(&mut state, &mut trace, |envelope| {
        envelope.from == NodeId(1)
            && envelope.to == NodeId(2)
            && matches!(envelope.message, Message::RequestVote(_))
    });
    apply_and_record(
        &mut state,
        &mut trace,
        Action::Tick(NodeId(2)),
        Operation::Tick(NodeId(2)),
    );
    apply_and_record(
        &mut state,
        &mut trace,
        Action::Tick(NodeId(2)),
        Operation::Tick(NodeId(2)),
    );
    record_delivery(&mut state, &mut trace, |envelope| {
        envelope.from == NodeId(2)
            && envelope.to == NodeId(1)
            && matches!(envelope.message, Message::RequestVoteResponse(_))
    });
    let proposal = Action::Propose {
        to: NodeId(1),
        proposal_id: ProposalId(1),
    };
    apply_and_record(
        &mut state,
        &mut trace,
        proposal,
        Operation::Propose {
            to: NodeId(1),
            proposal_id: ProposalId(1),
            stale_leader: false,
        },
    );

    let colliding = state
        .cluster()
        .network
        .iter()
        .enumerate()
        .filter_map(|(position, queued)| {
            (queued.envelope.from == NodeId(1)
                && queued.envelope.to == NodeId(3)
                && matches!(queued.envelope.message, Message::AppendEntries(_)))
            .then_some(position)
        })
        .collect::<Vec<_>>();
    assert_eq!(
        colliding.len(),
        2,
        "heartbeat and proposal append must collide"
    );
    let first = deliver_action(state.cluster(), colliding[0]);
    let second = deliver_action(state.cluster(), colliding[1]);
    assert_ne!(first, second);
    assert!(matches!(
        second,
        Action::Deliver {
            identity,
            ..
        } if identity.matching_ordinal() == 1
    ));
    trace.push(second);
    apply_to_state(&mut state, Operation::DeliverReadyAt(colliding[1]));
    let expected = summarize(state.cluster());

    let report = replay_raft_trace(
        configs,
        &trace,
        ReplayCheck::CommitSafety,
        ReplayExpectation::FinalState(&expected),
    )
    .expect("replay must select the second colliding append envelope");

    assert_eq!(report.state(), &expected);
}

fn apply_and_record(
    state: &mut ExplorationState,
    trace: &mut Vec<Action>,
    action: Action,
    operation: Operation,
) {
    trace.push(action);
    apply_to_state(state, operation);
}

fn record_delivery(
    state: &mut ExplorationState,
    trace: &mut Vec<Action>,
    predicate: impl Fn(&crate::Envelope) -> bool,
) {
    let position = state
        .cluster()
        .network
        .iter()
        .position(|queued| predicate(&queued.envelope))
        .expect("planned delivery must be queued");
    trace.push(deliver_action(state.cluster(), position));
    apply_to_state(state, Operation::DeliverReadyAt(position));
}
