//! Election timeout validation, deterministic jitter, and overflow behavior.

use super::*;

#[test]
fn zero_election_timeout_is_rejected() {
    assert_eq!(
        NodeConfig::new(NodeId(1), vec![NodeId(2)], 0).unwrap_err(),
        NodeConfigError::ZeroElectionTimeout
    );
    assert_eq!(
        NodeConfig::new_non_voter(NodeId(4), vec![NodeId(1)], 0).unwrap_err(),
        NodeConfigError::ZeroElectionTimeout
    );
}
#[test]
fn election_jitter_is_deterministic_and_spreads_symmetric_nodes() {
    let jittered = |id: u64| {
        let peers: Vec<NodeId> = [1, 2, 3]
            .into_iter()
            .filter(|peer| *peer != id)
            .map(NodeId)
            .collect();
        Node::new(
            NodeConfig::new(NodeId(id), peers, 4)
                .expect("valid config")
                .with_election_jitter_ticks(7),
        )
    };
    // Same id, same term: replays are exact.
    let mut first = jittered(1);
    let mut second = jittered(1);
    let ticks_until_candidacy = |node: &mut Node| {
        let mut ticks = 0;
        while node.role() == Role::Follower {
            let _ = node.step(Input::Tick);
            ticks += 1;
            assert!(ticks < 64, "node must eventually campaign");
        }
        ticks
    };
    assert_eq!(
        ticks_until_candidacy(&mut first),
        ticks_until_candidacy(&mut second)
    );

    // Symmetric peers diverge for at least one of several ids, breaking ties.
    let mut node_one = jittered(1);
    let mut node_two = jittered(2);
    let one = ticks_until_candidacy(&mut node_one);
    let two = ticks_until_candidacy(&mut node_two);
    assert!(
        one != two || {
            // Extremely unlikely with a 0..=7 spread, but if equal in term
            // one, the next term must diverge for some id: check id 3 too.
            let mut node_three = jittered(3);
            ticks_until_candidacy(&mut node_three) != one
        },
        "jitter must spread symmetric candidates"
    );

    // Jitter never fires before the base timeout.
    assert!(one >= 4 && two >= 4);
}
#[test]
fn maximum_election_jitter_does_not_overflow() {
    let mut first = Node::new(
        NodeConfig::new(NodeId(1), vec![NodeId(2)], 1)
            .expect("valid config")
            .with_election_jitter_ticks(u64::MAX),
    );
    let mut second = Node::new(
        NodeConfig::new(NodeId(1), vec![NodeId(2)], 1)
            .expect("valid config")
            .with_election_jitter_ticks(u64::MAX),
    );

    assert_eq!(first.step(Input::Tick), second.step(Input::Tick));
    assert_eq!(first.role(), second.role());
}
#[test]
fn election_jitter_saturates_base_plus_offset_overflow() {
    for id in 1..=32 {
        let mut node = Node::new(
            NodeConfig::new(NodeId(id), Vec::new(), u64::MAX - 1)
                .expect("valid config")
                .with_election_jitter_ticks(7),
        );
        node.election.set_elapsed(u64::MAX - 1);
        let _ = node.step(Input::Tick);
    }
}
#[test]
fn election_elapsed_saturates_before_timeout_check() {
    let mut node = Node::new(
        NodeConfig::new(NodeId(1), vec![NodeId(2)], u64::MAX)
            .expect("valid config")
            .with_pre_vote(false),
    );
    node.election.set_elapsed(u64::MAX);

    let outputs = node.step(Input::Tick);

    assert_eq!(node.role(), Role::Candidate);
    assert_eq!(outputs.len(), 1);
}
