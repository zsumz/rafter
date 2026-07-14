//! Lease establishment, renewal, expiry, and dependency safety.

use super::super::helpers::elect_leader;
use super::*;
use crate::{AppendEntriesResponse, Message, PreVoteResponse, ReadId, RequestVoteResponse};
use rafter_invariant_test::{oracle_assert, oracle_assert_eq};

const ELECTION_TIMEOUT_TICKS: u64 = 8;
const LEASE_WINDOW_TICKS: u64 = ELECTION_TIMEOUT_TICKS / 2;

fn read_index(read_id: u64) -> Input {
    Input::ReadIndex {
        read_id: ReadId(read_id),
    }
}

/// A three-voter node with the lease opt-in; the default posture already
/// carries the pre-vote and check-quorum foundation the lease requires.
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
/// granted by node 2.
fn elect_with_pre_vote(node: &mut Node) {
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
        .expect("timeout starts a pre-vote round");

    let _ = node.step(Input::Message {
        from: NodeId(2),
        message: Message::PreVoteResponse(PreVoteResponse {
            term: proposed_term,
            voter_id: NodeId(2),
            vote_granted: true,
        }),
    });
    assert_eq!(node.role(), Role::Candidate);
    let _ = node.step(Input::Message {
        from: NodeId(2),
        message: Message::RequestVoteResponse(RequestVoteResponse {
            term: node.current_term(),
            voter_id: NodeId(2),
            vote_granted: true,
        }),
    });
    assert_eq!(node.role(), Role::Leader);
}

/// A lease-configured leader with a current-term commit. The quorum
/// acknowledgement that commits the entry also confirms the initial lease
/// checkpoint: any same-term round acknowledged by a quorum proves the
/// leadership the checkpoint's basis claims.
fn leader_with_commit_and_confirmed_lease() -> Node {
    let mut leader = lease_node(1, &[2, 3]);
    elect_with_pre_vote(&mut leader);
    assert!(
        !leader.read_lease_active(),
        "votes elect but do not confirm a lease round"
    );
    let _ = leader.step(Input::ClientProposal {
        payload: b"first".to_vec(),
    });
    let _ = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            term: leader.current_term(),
            follower_id: NodeId(2),
            success: true,
            match_index: LogIndex(1),
            sequence: 1,
        }),
    });
    assert_eq!(leader.commit_index(), LogIndex(1));
    leader
}

/// Ticks once and returns the broadcast round's sequence.
fn tick_round(leader: &mut Node) -> u64 {
    let outputs = leader.step(Input::Tick);
    outputs
        .iter()
        .find_map(|output| match output {
            Output::Send {
                message: Message::AppendEntries(request),
                ..
            } => Some(request.sequence),
            _ => None,
        })
        .expect("leader tick broadcasts")
}

fn ack(leader: &mut Node, follower: u64, sequence: u64) {
    let term = leader.current_term();
    let match_index = leader.last_log_index();
    let _ = leader.step(Input::Message {
        from: NodeId(follower),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            term,
            follower_id: NodeId(follower),
            success: true,
            match_index,
            sequence,
        }),
    });
}

#[test]
fn a_confirmed_lease_grants_barriers_without_a_round_trip() {
    let mut leader = leader_with_commit_and_confirmed_lease();
    oracle_assert!(leader.read_lease_active());

    let outputs = leader.step(read_index(42));
    oracle_assert_eq!(
        outputs,
        vec![Output::ReadIndexGranted {
            read_id: ReadId(42),
            read_index: LogIndex(1),
        }],
        "the barrier grants immediately, with nothing registered"
    );
    oracle_assert_eq!(leader.pending_read_count(), 0);
}

#[test]
fn the_lease_lapses_without_quorum_acknowledgements() {
    let mut leader = leader_with_commit_and_confirmed_lease();
    oracle_assert!(leader.read_lease_active());

    for _ in 0..LEASE_WINDOW_TICKS {
        let _ = leader.step(Input::Tick);
    }
    oracle_assert!(!leader.read_lease_active());

    let outputs = leader.step(read_index(43));
    oracle_assert!(
        outputs.iter().any(|output| matches!(
            output,
            Output::Send {
                message: Message::AppendEntries(_),
                ..
            }
        )),
        "a lapsed lease starts the read-index round trip immediately"
    );
    oracle_assert_eq!(leader.pending_read_count(), 1);
}

#[test]
fn the_lease_boundary_is_the_half_election_window() {
    let mut leader = leader_with_commit_and_confirmed_lease();

    for _ in 0..(LEASE_WINDOW_TICKS - 1) {
        let _ = leader.step(Input::Tick);
    }
    oracle_assert!(
        leader.read_lease_active(),
        "the final tick before the documented skew window still holds"
    );
    oracle_assert_eq!(
        leader.step(read_index(45)),
        vec![Output::ReadIndexGranted {
            read_id: ReadId(45),
            read_index: LogIndex(1),
        }]
    );

    let _ = leader.step(Input::Tick);
    oracle_assert!(
        !leader.read_lease_active(),
        "at the half-election-timeout boundary the lease must lapse"
    );
    let outputs = leader.step(read_index(46));
    oracle_assert!(
        outputs.iter().any(|output| matches!(
            output,
            Output::Send {
                message: Message::AppendEntries(_),
                ..
            }
        )),
        "the first request outside the bound takes the quorum round trip"
    );
    oracle_assert_eq!(leader.pending_read_count(), 1);
}

#[test]
fn quorum_acknowledgements_renew_the_lease_across_windows() {
    let mut leader = leader_with_commit_and_confirmed_lease();

    // Each window: tick within it and confirm the re-armed checkpoint with
    // a fresh quorum acknowledgement; the lease never lapses.
    for _ in 0..3 {
        let mut sequence = 0;
        for _ in 0..(LEASE_WINDOW_TICKS - 1) {
            sequence = tick_round(&mut leader);
        }
        ack(&mut leader, 2, sequence);
        assert!(leader.read_lease_active());
    }
}

#[test]
fn acknowledgements_of_rounds_before_the_checkpoint_do_not_confirm_it() {
    let mut leader = lease_node(1, &[2, 3]);
    elect_with_pre_vote(&mut leader);
    let sequence = tick_round(&mut leader);

    // Age the checkpoint past the window so it re-arms at a fresh basis and
    // sequence; the old round's acknowledgement is then too stale to count.
    for _ in 0..LEASE_WINDOW_TICKS {
        let _ = leader.step(Input::Tick);
    }
    ack(&mut leader, 2, sequence);
    oracle_assert!(
        !leader.read_lease_active(),
        "an acknowledgement of a pre-re-arm round proves nothing about the fresh basis"
    );
}

#[test]
fn the_lease_opt_in_is_inert_without_its_safety_foundation() {
    let config = NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 3)
        .expect("test Raft node config is valid")
        .with_lease_reads(true);
    oracle_assert!(
        config.lease_reads(),
        "the default posture carries the lease's safety foundation"
    );
    oracle_assert!(
        !config.clone().with_pre_vote(false).lease_reads(),
        "without pre-vote the opt-in reports disabled"
    );
    oracle_assert!(
        !config.clone().with_check_quorum(false).lease_reads(),
        "without check-quorum the opt-in reports disabled"
    );
    let degraded = config.with_pre_vote(false).with_check_quorum(false);
    oracle_assert!(!degraded.lease_reads());

    // Behaviorally: acknowledged rounds never activate the lease and
    // barriers take the read-index round trip.
    let mut leader = Node::new(degraded);
    let _ = elect_leader(&mut leader);
    ack(&mut leader, 2, 1);
    oracle_assert_eq!(leader.commit_index(), LogIndex(1));

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
        .expect("leader tick broadcasts");
    ack(&mut leader, 2, sequence);
    oracle_assert!(!leader.read_lease_active());

    let outputs = leader.step(read_index(44));
    oracle_assert!(outputs.iter().any(|output| matches!(
        output,
        Output::Send {
            message: Message::AppendEntries(_),
            ..
        }
    )));
    oracle_assert_eq!(leader.pending_read_count(), 1);
}
