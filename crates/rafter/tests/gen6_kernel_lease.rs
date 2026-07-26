//! Regression suite, adopted from the gen-6 red-team hunt: the leader lease
//! must not outlive the deposition the leader itself authorized with
//! `TimeoutNow`.
//!
//! `NodeConfig::with_lease_reads` justifies the lease's safety with:
//!   "The lease also relies on voters refusing to depose a live leader. The
//!    request therefore becomes effective only while pre-vote and check-quorum
//!    are both effective."
//!
//! `Node::handle_timeout_now` documents the waiver of exactly that refusal:
//!   "the real, term-incrementing election, bypassing pre-vote and leader
//!    stickiness -- that bypass is the message's entire purpose".
//!
//! The kernel's guard used to be `read_index_batch` rejecting barriers while
//! `leader.pending_transfer.is_some()`. That guard is total over the *local
//! transfer record*, which `tick_leadership_transfer` deletes after one
//! election timeout. It is not total over the lifetime of the authorization,
//! which is an already-emitted message with no expiry — so the lease is now
//! waived for the remainder of the term instead, and these tests pin the
//! difference. The scenario is deliberately a whole three-node execution
//! rather than a synthesized state: the authorization has to be genuinely on
//! the wire while the record ages out, and the deposition has to be genuinely
//! won, or the test would be asserting against a state the protocol cannot
//! reach.

use std::collections::BTreeMap;

use rafter::{Input, LogIndex, Message, Node, NodeConfig, NodeId, Output, ReadId, Role};

const ELECTION_TIMEOUT_TICKS: u64 = 8;

fn configured_node(id: u64, peers: &[u64], lease_reads: bool) -> Node {
    Node::new(
        NodeConfig::new(
            NodeId(id),
            peers.iter().copied().map(NodeId).collect(),
            ELECTION_TIMEOUT_TICKS,
        )
        .expect("valid config")
        .with_lease_reads(lease_reads),
    )
}

type Wire = (u64, u64, Message);

fn sends(from: u64, outputs: Vec<Output>) -> Vec<Wire> {
    outputs
        .into_iter()
        .filter_map(|output| match output {
            Output::Send { to, message } => Some((from, to.0, message)),
            _ => None,
        })
        .collect()
}

struct Net {
    nodes: BTreeMap<u64, Node>,
    /// Messages the asynchronous network is still carrying.
    held: Vec<Wire>,
    hold_timeout_now: bool,
    isolated: Vec<u64>,
}

impl Net {
    fn new() -> Self {
        Self::with_lease_reads(true)
    }

    fn with_lease_reads(lease_reads: bool) -> Self {
        let mut nodes = BTreeMap::new();
        nodes.insert(1, configured_node(1, &[2, 3], lease_reads));
        nodes.insert(2, configured_node(2, &[1, 3], lease_reads));
        nodes.insert(3, configured_node(3, &[1, 2], lease_reads));
        Self {
            nodes,
            held: Vec::new(),
            hold_timeout_now: false,
            isolated: Vec::new(),
        }
    }

    fn node(&mut self, id: u64) -> &mut Node {
        self.nodes.get_mut(&id).expect("node exists")
    }

    fn step(&mut self, id: u64, input: Input) -> Vec<Output> {
        let outputs = self.node(id).step(input);
        let queue = sends(id, outputs.clone());
        self.pump(queue);
        outputs
    }

    /// Steps one node without routing its sends: used once node 1 is cut off
    /// from the rest of the cluster.
    fn step_local(&mut self, id: u64, input: Input) -> Vec<Output> {
        self.node(id).step(input)
    }

    fn pump(&mut self, mut queue: Vec<Wire>) {
        for _ in 0..64 {
            if queue.is_empty() {
                return;
            }
            let mut next = Vec::new();
            for (from, to, message) in queue.drain(..) {
                // An isolated node receives nothing. Messages it emitted
                // before the partition are still on the wire.
                if self.isolated.contains(&to) {
                    continue;
                }
                if self.hold_timeout_now && matches!(message, Message::TimeoutNow(_)) {
                    self.held.push((from, to, message));
                    continue;
                }
                let outputs = self.node(to).step(Input::Message {
                    from: NodeId(from),
                    message,
                });
                next.extend(sends(to, outputs));
            }
            queue = next;
        }
        panic!("message pump did not quiesce");
    }

    fn deliver_held(&mut self) {
        self.hold_timeout_now = false;
        let queue = std::mem::take(&mut self.held);
        assert!(!queue.is_empty(), "a TimeoutNow should have been held");
        self.pump(queue);
    }
}

/// Elects node 1 and commits one application entry in its term, leaving a
/// confirmed lease.
fn healthy_lease_leader() -> Net {
    healthy_leader(Net::new())
}

fn healthy_leader(mut net: Net) -> Net {
    for _ in 0..ELECTION_TIMEOUT_TICKS {
        let _ = net.step(1, Input::Tick);
    }
    assert_eq!(net.node(1).role(), Role::Leader, "node 1 wins its election");

    let _ = net.step(
        1,
        Input::ClientProposal {
            payload: b"v1".to_vec(),
        },
    );
    assert_eq!(net.node(1).commit_index(), LogIndex(2));
    net
}

/// Control: the same execution with the lease fast path disabled. The
/// quorum-confirmed `ReadIndex` path must not grant synchronously, because a
/// grant requires acknowledgement of a round broadcast after registration and
/// the followers have moved to the new term.
#[test]
fn quorum_read_after_an_expired_transfer_is_not_granted() {
    let mut net = healthy_leader(Net::with_lease_reads(false));
    assert!(!net.node(1).read_lease_active(), "lease path is off");

    net.hold_timeout_now = true;
    let _ = net.step(1, Input::TransferLeadership { target: NodeId(2) });
    for _ in 0..(4 * ELECTION_TIMEOUT_TICKS) {
        let _ = net.step(1, Input::Tick);
    }
    assert_eq!(net.node(1).role(), Role::Leader);

    net.isolated.push(1);
    net.deliver_held();
    assert_eq!(net.node(2).role(), Role::Leader);
    let _ = net.step(
        2,
        Input::ClientProposal {
            payload: b"v2".to_vec(),
        },
    );

    let outputs = net.step_local(
        1,
        Input::ReadIndex {
            read_id: ReadId(300),
        },
    );
    assert!(
        !outputs
            .iter()
            .any(|output| matches!(output, Output::ReadIndexGranted { .. })),
        "the quorum path must take a round trip: {outputs:?}"
    );
}

#[test]
fn lease_read_is_refused_while_the_transfer_record_exists() {
    // Baseline: the guard the kernel does have.
    let mut net = healthy_lease_leader();
    assert!(net.node(1).read_lease_active());
    net.hold_timeout_now = true;
    let _ = net.step(1, Input::TransferLeadership { target: NodeId(2) });

    let outputs = net.step(
        1,
        Input::ReadIndex {
            read_id: ReadId(100),
        },
    );
    assert!(
        outputs
            .iter()
            .any(|output| matches!(output, Output::ReadIndexRejected { .. })),
        "reads are refused while the transfer record lives: {outputs:?}"
    );
}

#[test]
fn lease_read_after_an_expired_transfer_returns_a_stale_index() {
    let mut net = healthy_lease_leader();
    let stale_commit = net.node(1).commit_index();

    // Node 1 authorizes node 2 to depose it immediately. The network is slow:
    // the TimeoutNow is still in flight. Nothing in Raft bounds that delay.
    net.hold_timeout_now = true;
    let _ = net.step(1, Input::TransferLeadership { target: NodeId(2) });
    assert_eq!(net.held.len(), 1, "the TimeoutNow is on the wire");

    // Healthy leadership while the authorization sits on the wire: node 1
    // heartbeats, both followers acknowledge, the lease checkpoint machine
    // confirms every round, and `tick_leadership_transfer` counts the transfer
    // record down to zero after one election timeout. Ticking well past that
    // shows the exposure is not a bounded window -- the record is gone, the
    // message is not.
    for _ in 0..(4 * ELECTION_TIMEOUT_TICKS) {
        let _ = net.step(1, Input::Tick);
    }
    assert_eq!(net.node(1).role(), Role::Leader);

    // The transfer record really is gone: the leader accepts proposals again,
    // which it refuses while a transfer is live. So nothing local remembers
    // the authorization, and the waiver cannot be the record under another
    // name.
    let outputs = net.step(
        1,
        Input::ClientProposal {
            payload: b"post-transfer".to_vec(),
        },
    );
    assert!(
        !outputs
            .iter()
            .any(|output| matches!(output, Output::RejectProposal { .. })),
        "the abandoned transfer must leave the leader operational: {outputs:?}"
    );
    let stale_commit = net.node(1).commit_index().max(stale_commit);

    // What survives is the waiver. The lease is void for the rest of the term,
    // and a barrier takes the quorum round trip instead of short-circuiting.
    assert!(
        !net.node(1).read_lease_active(),
        "an emitted TimeoutNow waives the lease for the rest of the term"
    );
    let outputs = net.step(
        1,
        Input::ReadIndex {
            read_id: ReadId(200),
        },
    );
    assert!(
        !outputs
            .iter()
            .any(|output| matches!(output, Output::ReadIndexRejected { .. })),
        "the leader is operational, so the barrier is not refused: {outputs:?}"
    );
    assert!(
        outputs.iter().any(|output| matches!(
            output,
            Output::Send {
                message: Message::AppendEntries(_),
                ..
            }
        )),
        "the barrier takes the quorum round trip rather than the lease: {outputs:?}"
    );
    // With both followers reachable the round trip completes inside the pump,
    // so this read is answered -- by quorum evidence, not by the lease. The
    // grant is emitted on node 1's response step, which `pump` consumes, so
    // the observable form is that nothing is left pending.
    assert_eq!(
        net.node(1).pending_read_count(),
        0,
        "a reachable leader still answers reads through the round trip"
    );

    // Now the network finally delivers the authorization node 1 issued.
    // Node 1 is partitioned from the new term, exactly as an old leader is.
    net.isolated.push(1);
    net.deliver_held();
    assert_eq!(
        net.node(2).role(),
        Role::Leader,
        "TimeoutNow bypasses pre-vote and stickiness by design"
    );

    // The new leader commits a write the old leader has never seen.
    let _ = net.step(
        2,
        Input::ClientProposal {
            payload: b"v2".to_vec(),
        },
    );
    let fresh_commit = net.node(2).commit_index();
    assert!(
        fresh_commit > stale_commit,
        "term {:?} leader committed past {stale_commit}",
        net.node(2).current_term()
    );

    // Node 1 still believes it is leader and still has no transfer record. A
    // linearizable read here must not be served.
    assert_eq!(net.node(1).role(), Role::Leader);
    assert!(!net.node(1).read_lease_active());
    let outputs = net.step_local(
        1,
        Input::ReadIndex {
            read_id: ReadId(201),
        },
    );

    let granted: Vec<LogIndex> = outputs
        .iter()
        .filter_map(|output| match output {
            Output::ReadIndexGranted { read_index, .. } => Some(*read_index),
            _ => None,
        })
        .collect();
    assert!(
        granted.is_empty(),
        "STALE LEASE READ: deposed leader {:?} granted read_index {:?} while \
         the term-{:?} leader had already committed through {fresh_commit}",
        net.node(1).id(),
        granted,
        net.node(2).current_term(),
    );
}
