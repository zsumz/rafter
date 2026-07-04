use super::super::*;
use super::helpers::{elect_leader, node};
use crate::{
    AppendEntries, AppendEntriesResponse, LocalProposalId, LogEntry, Message, ReadId,
    RequestVoteResponse,
};

/// A three-voter leader that has committed one entry in its current term,
/// making it eligible to serve read barriers.
fn leader_with_current_term_commit() -> Node {
    commit_first_entry(node(1, &[2, 3]))
}

/// Elects `leader` and commits one current-term entry through node 2's
/// acknowledgement.
fn commit_first_entry(mut leader: Node) -> Node {
    let _ = elect_leader(&mut leader);
    let _ = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            term: leader.current_term(),
            follower_id: NodeId(2),
            success: true,
            match_index: LogIndex(1),
            sequence: 0,
        }),
    });
    assert_eq!(leader.commit_index(), LogIndex(1));
    leader
}

fn ack(leader: &mut Node, follower: u64, sequence: u64) -> Vec<Output> {
    let term = leader.current_term();
    let match_index = leader.last_log_index();
    leader.step(Input::Message {
        from: NodeId(follower),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            term,
            follower_id: NodeId(follower),
            success: true,
            match_index,
            sequence,
        }),
    })
}

fn heartbeat_round(outputs: &[Output]) -> u64 {
    outputs
        .iter()
        .find_map(|output| match output {
            Output::Send {
                message: Message::AppendEntries(AppendEntries { sequence, .. }),
                ..
            } => Some(*sequence),
            _ => None,
        })
        .expect("leader tick broadcasts a heartbeat")
}

fn read_index(read_id: u64) -> Input {
    Input::ReadIndex {
        read_id: ReadId(read_id),
    }
}

fn granted(outputs: &[Output]) -> Vec<(ReadId, LogIndex)> {
    outputs
        .iter()
        .filter_map(|output| match output {
            Output::ReadIndexGranted {
                read_id,
                read_index,
            } => Some((*read_id, *read_index)),
            _ => None,
        })
        .collect()
}

fn canceled(outputs: &[Output]) -> Vec<(ReadId, ReadIndexCancelReason)> {
    outputs
        .iter()
        .filter_map(|output| match output {
            Output::ReadIndexCanceled { read_id, reason } => Some((*read_id, *reason)),
            _ => None,
        })
        .collect()
}

#[test]
fn read_rejected_without_current_term_commit() {
    let mut leader = node(1, &[2, 3]);
    let _ = elect_leader(&mut leader);

    let outputs = leader.step(read_index(7));

    assert!(matches!(
        outputs.as_slice(),
        [Output::ReadIndexRejected {
            read_id: ReadId(7),
            reason: ReadIndexRejection::NoCommitInCurrentTerm,
        }]
    ));
}

#[test]
fn read_rejected_on_follower() {
    let mut follower = node(2, &[1, 3]);
    let outputs = follower.step(read_index(3));
    assert!(matches!(
        outputs.as_slice(),
        [Output::ReadIndexRejected {
            read_id: ReadId(3),
            reason: ReadIndexRejection::NotLeader { .. },
        }]
    ));
}

#[test]
fn read_index_broadcasts_confirmation_round_immediately() {
    let mut leader = leader_with_current_term_commit();

    let heartbeats = leader.step(read_index(42));
    assert_eq!(leader.pending_read_count(), 1);
    let Output::Send {
        message: Message::AppendEntries(AppendEntries { sequence, .. }),
        ..
    } = &heartbeats[0]
    else {
        panic!("expected heartbeat");
    };
    let round = *sequence;

    let outputs = ack(&mut leader, 2, round);
    assert_eq!(granted(&outputs), vec![(ReadId(42), LogIndex(1))]);
    assert_eq!(leader.pending_read_count(), 0);
}

#[test]
fn changing_read_id_does_not_affect_quorum_behavior() {
    let mut first = leader_with_current_term_commit();
    let mut second = leader_with_current_term_commit();

    let first_round = heartbeat_round(&first.step(read_index(100)));
    let second_round = heartbeat_round(&second.step(read_index(200)));
    assert_eq!(first_round, second_round);
    assert_eq!(first.pending_read_count(), second.pending_read_count());

    let first_outputs = ack(&mut first, 2, first_round);
    let second_outputs = ack(&mut second, 2, second_round);

    assert_eq!(granted(&first_outputs), vec![(ReadId(100), LogIndex(1))]);
    assert_eq!(granted(&second_outputs), vec![(ReadId(200), LogIndex(1))]);
    assert_eq!(first.pending_read_count(), second.pending_read_count());
    assert_eq!(first.commit_index(), second.commit_index());
}

#[test]
fn leader_noop_unlocks_read_index_without_client_write() {
    let mut leader = node(1, &[2, 3]);
    let _ = elect_leader(&mut leader);
    assert_eq!(
        leader.log_entries_from(LogIndex(1)),
        vec![LogEntry::noop(leader.current_term())]
    );

    let outputs = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            term: leader.current_term(),
            follower_id: NodeId(2),
            success: true,
            match_index: LogIndex(1),
            sequence: 0,
        }),
    });
    assert_eq!(leader.commit_index(), LogIndex(1));
    assert!(outputs
        .iter()
        .all(|output| !matches!(output, Output::Apply { .. })));

    let heartbeats = leader.step(read_index(77));
    let round = heartbeat_round(&heartbeats);
    let outputs = ack(&mut leader, 2, round);

    assert_eq!(granted(&outputs), vec![(ReadId(77), LogIndex(1))]);
}

#[test]
fn delayed_ack_from_an_older_round_never_confirms_a_barrier() {
    let mut leader = leader_with_current_term_commit();

    // Observe the last pre-registration round from a heartbeat.
    let pre_round = heartbeat_round(&leader.step(Input::Tick));
    let post_round = heartbeat_round(&leader.step(read_index(9)));

    // Delayed echoes of pre-registration rounds must not count — even a
    // quorum of them proves nothing about leadership after registration.
    let outputs = ack(&mut leader, 2, pre_round);
    assert!(granted(&outputs).is_empty());
    let outputs = ack(&mut leader, 3, pre_round);
    assert!(granted(&outputs).is_empty());
    assert_eq!(leader.pending_read_count(), 1);

    // An echo of the eagerly broadcast post-registration round confirms.
    assert!(post_round > pre_round);
    let outputs = ack(&mut leader, 2, post_round);
    assert_eq!(granted(&outputs), vec![(ReadId(9), LogIndex(1))]);
}

#[test]
fn zero_sequence_echo_never_confirms_a_barrier() {
    let mut leader = leader_with_current_term_commit();
    let _ = leader.step(read_index(1));

    // A directly constructed or non-codec message with no round information
    // echoes zero.
    let outputs = ack(&mut leader, 2, 0);
    assert!(granted(&outputs).is_empty());
    assert_eq!(leader.pending_read_count(), 1);
}

#[test]
fn isolated_ex_leader_never_grants_a_read() {
    // The scenario needs the isolated leader to keep believing in itself for
    // the whole run, which is exactly what check-quorum forecloses.
    let mut leader = commit_first_entry(Node::new(
        NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 3)
            .expect("valid config")
            .with_check_quorum(false),
    ));

    // Partitioned: the read is registered, heartbeats go nowhere, no
    // acknowledgement ever arrives.
    let outputs = leader.step(read_index(5));
    assert!(granted(&outputs).is_empty());
    for _ in 0..20 {
        let outputs = leader.step(Input::Tick);
        assert!(
            granted(&outputs).is_empty(),
            "an unconfirmed leader must never grant a read barrier"
        );
    }
    assert_eq!(leader.pending_read_count(), 1);
}

#[test]
fn multiple_reads_grant_in_registration_order() {
    let mut leader = leader_with_current_term_commit();

    let _ = leader.step(read_index(1));
    // A second entry commits, then a second read registers at the higher index.
    let _ = leader.step(Input::ClientProposal {
        payload: b"second".to_vec(),
    });
    let _ = ack(&mut leader, 2, 0); // advances match/commit via match_index
    assert_eq!(leader.commit_index(), LogIndex(2));
    let round = heartbeat_round(&leader.step(read_index(2)));

    // One ack at the latest round confirms both barriers, in order.
    let outputs = ack(&mut leader, 2, round);
    assert_eq!(
        granted(&outputs),
        vec![(ReadId(1), LogIndex(1)), (ReadId(2), LogIndex(2))]
    );
}

#[test]
fn single_voter_grants_immediately() {
    let mut solo = Node::new(NodeConfig::new(NodeId(1), vec![], 3).expect("single voter config"));
    for _ in 0..3 {
        let _ = solo.step(Input::Tick);
    }
    assert_eq!(solo.role(), Role::Leader);
    assert_eq!(solo.commit_index(), LogIndex(1));

    let outputs = solo.step(read_index(11));
    assert_eq!(granted(&outputs), vec![(ReadId(11), LogIndex(1))]);
}

#[test]
fn reads_rejected_during_leadership_transfer() {
    let mut leader = leader_with_current_term_commit();
    let _ = leader.step(Input::TransferLeadership { target: NodeId(2) });

    let outputs = leader.step(read_index(4));
    assert!(matches!(
        outputs.as_slice(),
        [Output::ReadIndexRejected {
            read_id: ReadId(4),
            reason: ReadIndexRejection::LeadershipTransferInProgress { target: NodeId(2) },
        }]
    ));
}

#[test]
fn pending_reads_are_cleared_on_step_down() {
    let mut leader = leader_with_current_term_commit();
    let _ = leader.step(read_index(6));
    let _ = leader.step(read_index(7));
    assert_eq!(leader.pending_read_count(), 2);

    let outputs = leader.step(Input::Message {
        from: NodeId(3),
        message: Message::AppendEntries(AppendEntries {
            term: leader.current_term().next(),
            leader_id: NodeId(3),
            prev_log_index: LogIndex::ZERO,
            prev_log_term: Term::default(),
            entries: Vec::new(),
            leader_commit: LogIndex::ZERO,
            sequence: 1,
        }),
    });

    assert_eq!(leader.role(), Role::Follower);
    assert_eq!(leader.pending_read_count(), 0);
    assert_eq!(
        canceled(&outputs),
        vec![
            (ReadId(6), ReadIndexCancelReason::LeadershipLost),
            (ReadId(7), ReadIndexCancelReason::LeadershipLost),
        ]
    );
}

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
    assert!(leader.volatile.local_proposals.contains_key(&LogIndex(2)));

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

#[test]
fn pending_reads_are_capped() {
    let mut leader = leader_with_current_term_commit();
    for request_id in 0..1024 {
        let outputs = leader.step(read_index(request_id));
        assert!(granted(&outputs).is_empty());
    }
    let outputs = leader.step(read_index(9999));
    assert!(matches!(
        outputs.as_slice(),
        [Output::ReadIndexRejected {
            read_id: ReadId(9999),
            reason: ReadIndexRejection::TooManyPendingReads,
        }]
    ));
}
