//! Check-quorum evidence and isolated-leader step-down.

use super::support::*;

#[test]
fn check_quorum_steps_down_an_isolated_leader() {
    let mut leader = node(1, &[2, 3]);
    let _ = elect_leader(&mut leader);

    // No follower responses arrive; after one election timeout of leader
    // ticks the check-quorum window closes and the leader steps down in its
    // own term (thesis 6.2).
    for _ in 0..leader.config.election_timeout_ticks() {
        assert_eq!(leader.role(), Role::Leader);
        let _ = leader.step(Input::Tick);
    }
    assert_eq!(leader.role(), Role::Follower);
}

#[test]
fn one_tick_timeout_normalizes_check_quorum_before_leader_tick() {
    let mut leader = Node::new(
        NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 1)
            .expect("one-tick timeout remains valid")
            .with_pre_vote(false),
    );

    let campaign_outputs = leader.step(Input::Tick);
    assert!(campaign_outputs.iter().any(|output| matches!(
        output,
        Output::Send {
            message: Message::RequestVote(_),
            ..
        }
    )));

    let _ = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::RequestVoteResponse(RequestVoteResponse {
            term: leader.current_term(),
            voter_id: NodeId(2),
            vote_granted: true,
        }),
    });
    assert_eq!(leader.role(), Role::Leader);
    assert!(!leader.config.check_quorum());

    let heartbeat_outputs = leader.step(Input::Tick);

    assert_eq!(leader.role(), Role::Leader);
    assert!(heartbeat_outputs.iter().any(|output| matches!(
        output,
        Output::Send {
            message: Message::AppendEntries(_),
            ..
        }
    )));
}

#[test]
fn check_quorum_stepdown_cancels_pending_reads() {
    let mut leader = leader_with_current_term_commit();
    let _ = leader.step(read_index(60));
    assert_eq!(leader.pending_read_count(), 1);

    let mut cancel_outputs = Vec::new();
    for _ in 0..leader.config.election_timeout_ticks() * 2 {
        cancel_outputs = leader.step(Input::Tick);
        if leader.role() == Role::Follower {
            break;
        }
    }

    assert_eq!(leader.role(), Role::Follower);
    assert_eq!(leader.pending_read_count(), 0);
    assert_eq!(
        canceled(&cancel_outputs),
        vec![(ReadId(60), ReadIndexCancelReason::LeadershipLost)]
    );
}

#[test]
fn check_quorum_stepdown_drops_tracked_local_proposals() {
    let mut leader = node(1, &[2, 3]);
    let _ = elect_leader(&mut leader);

    let proposal_id = LocalProposalId(99);
    let _ = leader.step(Input::TrackedClientProposal {
        proposal_id,
        payload: b"uncommitted".to_vec(),
    });
    assert!(leader.volatile.local_proposals.contains_key(LogIndex(2)));

    let mut outputs = Vec::new();
    for _ in 0..leader.config.election_timeout_ticks() {
        outputs = leader.step(Input::Tick);
        if leader.role() == Role::Follower {
            break;
        }
    }

    assert_eq!(leader.role(), Role::Follower);
    assert!(leader.volatile.local_proposals.is_empty());
    assert!(outputs.iter().any(|output| matches!(
        output,
        Output::LocalProposalDropped {
            proposal_id: LocalProposalId(99),
            index: LogIndex(2),
            term: Term(1),
            reason: LocalProposalDropReason::LeadershipLost,
        }
    )));
}

#[test]
fn check_quorum_keeps_a_leader_with_a_responsive_quorum() {
    let mut leader = node(1, &[2, 3]);
    let _ = elect_leader(&mut leader);

    for _ in 0..3 {
        for _ in 0..leader.config.election_timeout_ticks() - 1 {
            let _ = leader.step(Input::Tick);
            let _ = ack(&mut leader, 2, 0);
        }
        let _ = leader.step(Input::Tick);
        assert_eq!(leader.role(), Role::Leader);
    }
}

#[test]
fn check_quorum_off_preserves_existing_behavior() {
    let mut leader = Node::new(
        NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 3)
            .expect("valid config")
            .with_check_quorum(false),
    );
    let _ = elect_leader(&mut leader);

    for _ in 0..50 {
        let _ = leader.step(Input::Tick);
    }
    assert_eq!(leader.role(), Role::Leader);
}

#[test]
fn check_quorum_step_down_forgets_the_leader_hint() {
    let mut leader = node(1, &[2, 3]);
    // Elect via pre-vote then real votes.
    for _ in 0..3 {
        let _ = leader.step(Input::Tick);
    }
    let _ = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::PreVoteResponse(crate::PreVoteResponse {
            term: leader.current_term().next(),
            voter_id: NodeId(2),
            vote_granted: true,
        }),
    });
    let _ = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::RequestVoteResponse(crate::RequestVoteResponse {
            term: leader.current_term(),
            voter_id: NodeId(2),
            vote_granted: true,
        }),
    });
    assert_eq!(leader.role(), Role::Leader);

    for _ in 0..3 {
        let _ = leader.step(Input::Tick);
    }
    assert_eq!(leader.role(), Role::Follower);
    // Having just proven no quorum hears it, the node must grant pre-votes
    // immediately rather than vouching for its own dead leadership.
    assert_eq!(leader.leader_hint(), None);
}
