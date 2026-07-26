//! The leader lease after a deposition this leader authorized.
//!
//! A `TimeoutNow` waives the refusal the lease rests on, for one voter, with
//! no expiry. These scenarios pin the four boundaries of the term-scoped
//! waiver that replaces the local transfer record as the guard: where it is
//! armed, where it is not, what it does and does not suspend, and what clears
//! it.

use super::support::*;
use rafter_invariant_test::{oracle_assert, oracle_assert_eq};

const ELECTION_TIMEOUT_TICKS: u64 = 8;

fn lease_node(id: u64, peers: &[u64]) -> Node {
    Node::new(
        NodeConfig::new(
            NodeId(id),
            peers.iter().copied().map(NodeId).collect(),
            ELECTION_TIMEOUT_TICKS,
        )
        .expect("test Raft node config is valid")
        .with_lease_reads(true),
    )
}

/// Elects `node` through the pre-vote round plus the real election, both
/// granted by node 2. The shared `elect_leader` helper is pinned to a
/// three-tick timeout; the lease window is derived from the timeout, so these
/// scenarios need a longer one.
fn elect(node: &mut Node) {
    let mut outputs = Vec::new();
    for _ in 0..ELECTION_TIMEOUT_TICKS {
        outputs = node.step(Input::Tick);
    }
    assert_eq!(node.role(), Role::PreCandidate);
    let proposed_term = outputs
        .iter()
        .find_map(|output| match output {
            Output::Send {
                message: Message::PreVote(request),
                ..
            } => Some(request.term),
            _ => None,
        })
        .expect("the timeout starts a pre-vote round");
    let _ = node.step(Input::Message {
        from: NodeId(2),
        message: Message::PreVoteResponse(crate::PreVoteResponse {
            term: proposed_term,
            voter_id: NodeId(2),
            vote_granted: true,
        }),
    });
    let term = node.current_term();
    let _ = node.step(Input::Message {
        from: NodeId(2),
        message: Message::RequestVoteResponse(RequestVoteResponse {
            term,
            voter_id: NodeId(2),
            vote_granted: true,
        }),
    });
    assert_eq!(node.role(), Role::Leader);
}

/// A lease-configured leader whose current-term entry is committed and whose
/// lease checkpoint is confirmed by the same acknowledgement.
fn lease_leader() -> Node {
    let mut leader = lease_node(1, &[2, 3]);
    elect(&mut leader);
    acknowledge(&mut leader, NodeId(2), LogIndex(1), 0);
    assert_eq!(leader.commit_index(), LogIndex(1));
    assert!(
        leader.read_lease_active(),
        "the fixture starts with a lease"
    );
    leader
}

fn acknowledge(leader: &mut Node, follower: NodeId, match_index: LogIndex, sequence: u64) {
    let term = leader.current_term();
    let _ = leader.step(Input::Message {
        from: follower,
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            term,
            follower_id: follower,
            success: true,
            match_index,
            sequence,
        }),
    });
}

/// Ticks past the transfer's abort deadline so the pending record is gone.
fn abandon_transfer(leader: &mut Node) {
    for _ in 0..ELECTION_TIMEOUT_TICKS {
        let _ = leader.step(Input::Tick);
    }
    assert!(
        leader.leader.pending_transfer.is_none(),
        "one election timeout abandons the transfer record"
    );
}

/// Renews the lease across the ticks spent abandoning the transfer, so a
/// lapsed window can never be mistaken for the waiver.
fn renew_lease(leader: &mut Node) {
    let outputs = leader.step(Input::Tick);
    let sequence = outputs
        .iter()
        .find_map(|output| match output {
            Output::Send {
                message: Message::AppendEntries(request),
                ..
            } => Some(request.sequence),
            _ => None,
        })
        .expect("a leader tick broadcasts");
    let match_index = leader.last_log_index();
    acknowledge(leader, NodeId(2), match_index, sequence);
}

#[test]
fn an_emitted_timeout_now_waives_the_lease_past_the_abandoned_transfer() {
    let mut leader = lease_leader();
    let outputs = leader.step(Input::TransferLeadership { target: NodeId(2) });
    oracle_assert!(
        timeout_now_to(&outputs, NodeId(2)).is_some(),
        "a caught-up target is authorized immediately"
    );

    abandon_transfer(&mut leader);
    renew_lease(&mut leader);

    oracle_assert!(
        leader
            .leader
            .lease
            .holds(leader.leader.ticks, leader.config.read_lease_ticks()),
        "the lease timer itself is healthy, so only the waiver can suspend it"
    );
    oracle_assert!(
        !leader.read_lease_active(),
        "the authorization outlives the record that used to guard the lease"
    );
    oracle_assert!(
        leader
            .step(Input::ReadIndex { read_id: ReadId(1) })
            .iter()
            .all(|output| !matches!(output, Output::ReadIndexGranted { .. })),
        "no barrier grants from a waived lease"
    );
}

#[test]
fn a_transfer_that_never_authorized_anything_leaves_the_lease_intact() {
    // Node 2 lags, so the transfer request writes no TimeoutNow and waives
    // nothing. The record still ages out after one election timeout.
    let mut leader = lease_leader();
    leader
        .persistent
        .log
        .push(LogEntry::application(Term(1), b"lagging".to_vec()));
    let outputs = leader.step(Input::TransferLeadership { target: NodeId(2) });
    oracle_assert!(
        timeout_now_to(&outputs, NodeId(2)).is_none(),
        "a lagging target is not authorized yet"
    );

    abandon_transfer(&mut leader);
    renew_lease(&mut leader);

    oracle_assert!(
        leader.read_lease_active(),
        "a transfer that authorized nothing must not cost the lease"
    );
    oracle_assert_eq!(
        leader.step(Input::ReadIndex { read_id: ReadId(2) }),
        vec![Output::ReadIndexGranted {
            read_id: ReadId(2),
            read_index: leader.commit_index(),
        }]
    );
}

#[test]
fn a_waived_lease_still_answers_reads_through_the_quorum_round_trip() {
    let mut leader = lease_leader();
    let _ = leader.step(Input::TransferLeadership { target: NodeId(2) });
    abandon_transfer(&mut leader);
    renew_lease(&mut leader);

    // Refusal is the live-transfer rule, not the waiver's. An abandoned
    // transfer leaves an operational leader.
    let outputs = leader.step(Input::ReadIndex { read_id: ReadId(3) });
    oracle_assert!(
        outputs
            .iter()
            .all(|output| !matches!(output, Output::ReadIndexRejected { .. })),
        "the waiver suspends the fast path, it does not refuse reads: {outputs:?}"
    );
    let sequence = outputs
        .iter()
        .find_map(|output| match output {
            Output::Send {
                message: Message::AppendEntries(request),
                ..
            } => Some(request.sequence),
            _ => None,
        })
        .expect("the barrier registers a confirmation round");
    oracle_assert_eq!(leader.pending_read_count(), 1);

    let match_index = leader.last_log_index();
    let term = leader.current_term();
    let granted = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            term,
            follower_id: NodeId(2),
            success: true,
            match_index,
            sequence,
        }),
    });
    oracle_assert!(
        granted.iter().any(|output| matches!(
            output,
            Output::ReadIndexGranted {
                read_id: ReadId(3),
                ..
            }
        )),
        "quorum evidence still answers the barrier: {granted:?}"
    );
    oracle_assert_eq!(leader.pending_read_count(), 0);
}

#[test]
fn a_new_term_clears_the_waiver() {
    let mut leader = lease_leader();
    let _ = leader.step(Input::TransferLeadership { target: NodeId(2) });
    abandon_transfer(&mut leader);
    oracle_assert!(!leader.read_lease_active());

    // The transfer target's campaign deposes this node; it then wins the term
    // after. The waiver belongs to the leadership that emitted the message.
    let deposing_term = leader.current_term().next();
    let _ = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::RequestVote(RequestVote {
            term: deposing_term,
            candidate_id: NodeId(2),
            last_log_index: leader.last_log_index(),
            last_log_term: Term(1),
        }),
    });
    oracle_assert_eq!(leader.role(), Role::Follower);

    elect(&mut leader);
    oracle_assert_eq!(leader.role(), Role::Leader);
    oracle_assert!(
        !leader.leader.deposition_authorized,
        "leader state is rebuilt per term, and the waiver rides with it"
    );

    let noop_index = leader.last_log_index();
    acknowledge(&mut leader, NodeId(2), noop_index, 0);
    oracle_assert!(
        leader.read_lease_active(),
        "a fresh term starts with a fresh, unwaived lease"
    );
}
