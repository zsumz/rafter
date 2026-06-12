//! Local proposal correlation at the Raft kernel layer.
//!
//! `LocalProposalAppended` only says the leader appended the proposal locally.
//! Client-facing success still requires the later committed `Output::Apply`.
//! This example keeps request identity in memory; production clients still own
//! durable request IDs, retry policy, and unknown-outcome handling.
//!
//! Run with:
//!
//! ```text
//! cargo run -p rafter --example tracked_proposal
//! ```

use std::collections::VecDeque;

use rafter::{
    Input, LocalProposalId, LogIndex, Message, Node, NodeConfig, NodeId, Output, Role,
    SharedPayload, Term,
};

fn main() {
    let mut nodes = vec![
        Node::new(config(1, &[2, 3], 3)),
        Node::new(config(2, &[1, 3], 9)),
        Node::new(config(3, &[1, 2], 9)),
    ];
    let mut network = VecDeque::new();
    let mut appends = Vec::new();
    let mut applies = Vec::new();

    for _ in 0..3 {
        handle_outputs(
            NodeId(1),
            nodes[0].step(Input::Tick),
            &mut network,
            &mut appends,
            &mut applies,
        );
    }
    deliver_all(&mut nodes, &mut network, &mut appends, &mut applies);
    assert_eq!(nodes[0].role(), Role::Leader);
    appends.clear();
    applies.clear();

    let proposal_id = LocalProposalId(42);
    handle_outputs(
        NodeId(1),
        nodes[0].step(Input::TrackedClientProposal {
            proposal_id,
            payload: b"transfer:7->9:5".to_vec(),
        }),
        &mut network,
        &mut appends,
        &mut applies,
    );

    let append = appends
        .iter()
        .find(|event| event.proposal_id == proposal_id)
        .expect("tracked proposal emits a local append event");
    assert!(
        applies.is_empty(),
        "local append is not commit or client-facing success"
    );
    println!(
        "proposal {:?} locally appended at {:?}/{:?}; waiting for commit",
        append.proposal_id, append.index, append.term
    );

    deliver_all(&mut nodes, &mut network, &mut appends, &mut applies);
    for _ in 0..6 {
        if applies.len() == 3 {
            break;
        }
        handle_outputs(
            NodeId(1),
            nodes[0].step(Input::Tick),
            &mut network,
            &mut appends,
            &mut applies,
        );
        deliver_all(&mut nodes, &mut network, &mut appends, &mut applies);
    }

    let tracked_apply = applies
        .iter()
        .find(|event| event.local_proposal_id == Some(proposal_id))
        .expect("leader apply preserves the local proposal id");
    println!(
        "proposal {:?} committed and applied on {:?} at {:?}/{:?}: {}",
        proposal_id,
        tracked_apply.node_id,
        tracked_apply.index,
        tracked_apply.term,
        String::from_utf8_lossy(&tracked_apply.payload)
    );
    assert_eq!(
        tracked_apply.node_id,
        NodeId(1),
        "local proposal ids are only meaningful on the proposing node"
    );
}

fn config(id: u64, peers: &[u64], election_timeout_ticks: u64) -> NodeConfig {
    NodeConfig::new(
        NodeId(id),
        peers.iter().copied().map(NodeId).collect(),
        election_timeout_ticks,
    )
    .expect("static config is valid")
}

fn deliver_all(
    nodes: &mut [Node],
    network: &mut VecDeque<(NodeId, NodeId, Message)>,
    appends: &mut Vec<AppendEvent>,
    applies: &mut Vec<ApplyEvent>,
) {
    while let Some((from, to, message)) = network.pop_front() {
        let node = nodes
            .iter_mut()
            .find(|node| node.id() == to)
            .expect("message is addressed to a known node");
        handle_outputs(
            to,
            node.step(Input::Message { from, message }),
            network,
            appends,
            applies,
        );
    }
}

fn handle_outputs(
    from: NodeId,
    outputs: Vec<Output>,
    network: &mut VecDeque<(NodeId, NodeId, Message)>,
    appends: &mut Vec<AppendEvent>,
    applies: &mut Vec<ApplyEvent>,
) {
    for output in outputs {
        match output {
            Output::Send { to, message } => network.push_back((from, to, message)),
            Output::LocalProposalAppended {
                proposal_id,
                index,
                term,
            } => appends.push(AppendEvent {
                proposal_id,
                index,
                term,
            }),
            Output::Apply {
                index,
                term,
                payload,
                local_proposal_id,
            } => applies.push(ApplyEvent {
                node_id: from,
                index,
                term,
                payload,
                local_proposal_id,
            }),
            Output::RejectProposal {
                proposal_id,
                reason,
            } => println!("proposal {proposal_id:?} rejected by {from:?}: {reason:?}"),
            Output::LeadershipTransferRejected { reason, .. } => {
                println!("leadership transfer rejected by {from:?}: {reason:?}");
            }
            Output::ReadIndexGranted { .. }
            | Output::ReadIndexRejected { .. }
            | Output::ReadIndexCanceled { .. }
            | Output::LocalProposalDropped { .. }
            | Output::StageSnapshotChunk { .. }
            | Output::SendSnapshotChunk { .. }
            | Output::ApplySnapshot { .. } => {}
        }
    }
}

#[derive(Debug)]
struct AppendEvent {
    proposal_id: LocalProposalId,
    index: LogIndex,
    term: Term,
}

#[derive(Debug)]
struct ApplyEvent {
    node_id: NodeId,
    index: LogIndex,
    term: Term,
    payload: SharedPayload,
    local_proposal_id: Option<LocalProposalId>,
}
