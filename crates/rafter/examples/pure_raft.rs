//! Direct Layer 0 usage: three pure Raft nodes, an in-memory network, and an
//! opaque command.
//!
//! This demonstrates the kernel input/output boundary only. It deliberately
//! does not provide persistence, transport security, background tasks, or
//! application-state durability.
//!
//! Run with:
//!
//! ```text
//! cargo run -p rafter --example pure_raft
//! ```

use std::collections::VecDeque;

use rafter::{Input, LogIndex, Message, Node, NodeConfig, NodeId, Output, Role, SharedPayload};

fn main() {
    let mut nodes = vec![
        Node::new(config(1, &[2, 3], 3)),
        Node::new(config(2, &[1, 3], 9)),
        Node::new(config(3, &[1, 2], 9)),
    ];
    let mut network = VecDeque::new();
    let mut applied = Vec::new();

    for _ in 0..3 {
        handle_outputs(
            NodeId(1),
            nodes[0].step(Input::Tick),
            &mut network,
            &mut applied,
        );
    }
    deliver_all(&mut nodes, &mut network, &mut applied);

    assert_eq!(nodes[0].role(), Role::Leader);
    println!(
        "node 1 elected leader in term {:?}",
        nodes[0].current_term()
    );
    applied.clear();

    handle_outputs(
        NodeId(1),
        nodes[0].step(Input::ClientProposal {
            payload: b"set account:7 balance=42".to_vec(),
        }),
        &mut network,
        &mut applied,
    );
    deliver_all(&mut nodes, &mut network, &mut applied);
    for _ in 0..6 {
        if applied.len() == 3 {
            break;
        }
        handle_outputs(
            NodeId(1),
            nodes[0].step(Input::Tick),
            &mut network,
            &mut applied,
        );
        deliver_all(&mut nodes, &mut network, &mut applied);
    }

    for (node_id, index, payload) in &applied {
        println!(
            "node {node_id:?} applied {index:?}: {}",
            String::from_utf8_lossy(payload)
        );
    }
    assert_eq!(applied.len(), 3, "every node applied the committed entry");
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
    applied: &mut Vec<(NodeId, LogIndex, SharedPayload)>,
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
            applied,
        );
    }
}

fn handle_outputs(
    from: NodeId,
    outputs: Vec<Output>,
    network: &mut VecDeque<(NodeId, NodeId, Message)>,
    applied: &mut Vec<(NodeId, LogIndex, SharedPayload)>,
) {
    for output in outputs {
        match output {
            Output::Send { to, message } => network.push_back((from, to, message)),
            Output::Apply { index, payload, .. } => applied.push((from, index, payload)),
            Output::RejectProposal { reason, .. } => {
                println!("proposal rejected by {from:?}: {reason:?}");
            }
            Output::LeadershipTransferRejected { reason, .. } => {
                println!("leadership transfer rejected by {from:?}: {reason:?}");
            }
            Output::LocalProposalAppended { .. }
            | Output::LocalProposalDropped { .. }
            | Output::ReadIndexGranted { .. }
            | Output::ReadIndexRejected { .. }
            | Output::ReadIndexCanceled { .. }
            | Output::StageSnapshotChunk { .. }
            | Output::SendSnapshotChunk { .. }
            | Output::ApplySnapshot { .. } => {}
        }
    }
}
