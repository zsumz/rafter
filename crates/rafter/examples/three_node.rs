//! Three in-process Raft nodes electing a leader and committing a command.
//!
//! The kernel is sans-IO: this example owns the "network" (a message queue)
//! and the clock (explicit ticks), which is all any integration has to do.
//! It is not a persistence, transport security, or application-state
//! durability template.
//!
//! Run with:
//!
//! ```text
//! cargo run -p rafter --example three_node
//! ```

use rafter::{Input, Message, Node, NodeConfig, NodeId, Output, Role};

fn main() {
    // Node 1 has the shortest election timeout, so it becomes candidate first.
    let mut nodes: Vec<Node> = vec![
        Node::new(config(1, &[2, 3], 3)),
        Node::new(config(2, &[1, 3], 9)),
        Node::new(config(3, &[1, 2], 9)),
    ];

    // The caller owns time: tick node 1 past its election timeout.
    let mut inbox: Vec<(NodeId, NodeId, Message)> = Vec::new();
    for _ in 0..3 {
        route(NodeId(1), nodes[0].step(Input::Tick), &mut inbox);
    }
    deliver_until_quiet(&mut nodes, &mut inbox);
    assert_eq!(nodes[0].role(), Role::Leader);
    println!(
        "node 1 elected leader in term {:?}",
        nodes[0].current_term()
    );

    // Propose an opaque payload on the leader; the kernel replicates it and
    // every node emits Output::Apply once the entry commits.
    route(
        NodeId(1),
        nodes[0].step(Input::ClientProposal {
            payload: b"set x=1".to_vec(),
        }),
        &mut inbox,
    );
    let applied = deliver_until_quiet(&mut nodes, &mut inbox);
    for (node_id, index, payload) in &applied {
        println!(
            "node {node_id:?} applied index {index:?}: {}",
            String::from_utf8_lossy(payload)
        );
    }
    assert_eq!(
        applied.len(),
        3,
        "all three nodes apply the committed entry"
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

/// Queue every `Output::Send` for delivery; print rejections; collect applies.
fn route(
    from: NodeId,
    outputs: Vec<Output>,
    inbox: &mut Vec<(NodeId, NodeId, Message)>,
) -> Vec<(NodeId, rafter::LogIndex, rafter::SharedPayload)> {
    let mut applied = Vec::new();
    for output in outputs {
        match output {
            Output::Send { to, message } => inbox.push((from, to, message)),
            Output::Apply { index, payload, .. } => applied.push((from, index, payload)),
            Output::ApplySnapshot { .. }
            | Output::SendSnapshotChunk { .. }
            | Output::StageSnapshotChunk { .. }
            | Output::LocalProposalAppended { .. }
            | Output::LocalProposalDropped { .. }
            | Output::ReadIndexGranted { .. }
            | Output::ReadIndexRejected { .. }
            | Output::ReadIndexCanceled { .. } => {}
            Output::RejectProposal { reason, .. } => println!("rejected: {reason:?}"),
            Output::LeadershipTransferRejected { reason, .. } => {
                println!("transfer rejected: {reason:?}");
            }
        }
    }
    applied
}

/// Deliver queued messages until the network is quiet, collecting applies.
fn deliver_until_quiet(
    nodes: &mut [Node],
    inbox: &mut Vec<(NodeId, NodeId, Message)>,
) -> Vec<(NodeId, rafter::LogIndex, rafter::SharedPayload)> {
    let mut applied = Vec::new();
    while let Some((from, to, message)) = inbox.pop() {
        let node = nodes
            .iter_mut()
            .find(|node| node.id() == to)
            .expect("message addressed to a known node");
        let outputs = node.step(Input::Message { from, message });
        applied.extend(route(to, outputs, inbox));
    }
    applied
}
