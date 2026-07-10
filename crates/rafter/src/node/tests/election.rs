use super::super::*;
use super::helpers::{assert_append_entries, assert_vote_response, campaign, elect_leader, node};
use crate::{
    AppendEntries, AppendEntriesResponse, CommittedConfiguration, ConfigurationEntry,
    ConfigurationId, LogEntry, MembershipSet, RequestVote, RequestVoteResponse,
};

#[test]
fn node_starts_election_after_timeout() {
    let mut node = Node::new(
        NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 3)
            .expect("test Raft node config is valid")
            .with_pre_vote(false),
    );

    assert!(node.step(Input::Tick).is_empty());
    assert!(node.step(Input::Tick).is_empty());
    let outputs = node.step(Input::Tick);

    assert_eq!(node.role(), Role::Candidate);
    assert_eq!(node.current_term(), Term(1));
    assert_eq!(outputs.len(), 2);
}

#[test]
fn single_voter_leadership_noop_does_not_drop_prior_term_apply() {
    let mut node = Node::from_bootstrap(
        NodeConfig::new(NodeId(1), vec![], 1)
            .expect("single voter config is valid")
            .with_pre_vote(false),
        BootstrapState {
            current_term: Term(1),
            voted_for: None,
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
            snapshot: None,
            log: vec![BootstrapLogEntry::application(
                LogIndex(1),
                Term(1),
                b"old-entry".to_vec(),
            )],
        },
    )
    .expect("single voter bootstrap is valid");

    let outputs = node.step(Input::Tick);

    assert_eq!(node.role(), Role::Leader);
    assert_eq!(node.commit_index(), LogIndex(2));
    assert!(outputs.iter().any(|output| matches!(
        output,
        Output::Apply { index, payload, .. }
            if *index == LogIndex(1) && payload.as_ref() == b"old-entry"
    )));
    assert!(outputs.iter().all(|output| !matches!(
        output,
        Output::Apply {
            index: LogIndex(2),
            ..
        }
    )));
}

#[test]
fn single_voter_leadership_noop_is_broadcast_to_learners() {
    let committed_configuration = CommittedConfiguration {
        index: LogIndex(1),
        config_id: ConfigurationId(3),
    };
    let mut node = Node::from_bootstrap(
        NodeConfig::new(NodeId(1), vec![NodeId(2)], 1)
            .expect("single voter plus learner config is valid")
            .with_pre_vote(false),
        BootstrapState {
            current_term: Term(1),
            voted_for: None,
            commit_index: LogIndex(1),
            committed_configuration: Some(committed_configuration),
            snapshot: None,
            log: vec![BootstrapLogEntry::configuration(
                LogIndex(1),
                Term(1),
                ConfigurationEntry::stable(
                    committed_configuration.config_id,
                    MembershipSet::new(vec![NodeId(1)], vec![NodeId(2)])
                        .expect("membership with one learner is valid"),
                ),
            )],
        },
    )
    .expect("single voter plus learner bootstrap is valid");

    let outputs = node.step(Input::Tick);

    assert_eq!(node.role(), Role::Leader);
    assert_eq!(node.commit_index(), LogIndex(2));
    assert!(outputs.iter().any(|output| matches!(
        output,
        Output::Send {
            to: NodeId(2),
            message: Message::AppendEntries(AppendEntries {
                prev_log_index: LogIndex(1),
                entries,
                ..
            }),
        } if entries.as_slice() == [LogEntry::noop(Term(2))]
    )));
}

#[test]
fn follower_grants_one_vote_per_term() {
    let mut node = node(1, &[2, 3]);

    let first = node.step(Input::Message {
        from: NodeId(2),
        message: Message::RequestVote(RequestVote {
            term: Term(7),
            candidate_id: NodeId(2),
            last_log_index: LogIndex(0),
            last_log_term: Term(0),
        }),
    });
    let second = node.step(Input::Message {
        from: NodeId(3),
        message: Message::RequestVote(RequestVote {
            term: Term(7),
            candidate_id: NodeId(3),
            last_log_index: LogIndex(0),
            last_log_term: Term(0),
        }),
    });

    assert_vote_response(&first, NodeId(2), true);
    assert_vote_response(&second, NodeId(3), false);
}

#[test]
fn same_term_append_entries_step_down_preserves_recorded_vote() {
    let mut node = node(1, &[2, 3]);

    let _ = campaign(&mut node);
    assert_eq!(node.current_term(), Term(1));
    assert_eq!(node.voted_for(), Some(NodeId(1)));

    let append_outputs = node.step(Input::Message {
        from: NodeId(2),
        message: Message::AppendEntries(AppendEntries {
            sequence: 0,
            term: Term(1),
            leader_id: NodeId(2),
            prev_log_index: LogIndex::ZERO,
            prev_log_term: Term::default(),
            entries: Vec::new().into(),
            leader_commit: LogIndex::ZERO,
        }),
    });

    assert_eq!(node.role(), Role::Follower);
    assert_eq!(node.voted_for(), Some(NodeId(1)));
    assert!(matches!(
        append_outputs.as_slice(),
        [Output::Send {
            to: NodeId(2),
            message: Message::AppendEntriesResponse(response),
        }] if response.success
    ));

    let vote_outputs = node.step(Input::Message {
        from: NodeId(3),
        message: Message::RequestVote(RequestVote {
            term: Term(1),
            candidate_id: NodeId(3),
            last_log_index: LogIndex::ZERO,
            last_log_term: Term::default(),
        }),
    });

    assert_vote_response(&vote_outputs, NodeId(3), false);
    assert_eq!(node.voted_for(), Some(NodeId(1)));
}

#[test]
fn candidate_becomes_leader_after_quorum() {
    let mut node = node(1, &[2, 3]);

    let outputs = elect_leader(&mut node);

    assert_eq!(node.role(), Role::Leader);
    assert!(outputs.iter().any(|output| matches!(
        output,
        Output::Send {
            message: Message::AppendEntries(_),
            ..
        }
    )));
}

#[test]
fn candidate_rejects_vote_response_when_sender_disagrees_with_voter_id() {
    let mut node = node(1, &[2, 3]);

    let _ = campaign(&mut node);

    let outputs = node.step(Input::Message {
        from: NodeId(2),
        message: Message::RequestVoteResponse(RequestVoteResponse {
            term: node.current_term(),
            voter_id: NodeId(3),
            vote_granted: true,
        }),
    });

    assert!(outputs.is_empty());
    assert_eq!(node.role(), Role::Candidate);
}

#[test]
fn candidate_rejects_vote_response_from_unknown_voter() {
    let mut node = node(1, &[2, 3]);

    let _ = campaign(&mut node);

    let outputs = node.step(Input::Message {
        from: NodeId(9),
        message: Message::RequestVoteResponse(RequestVoteResponse {
            term: node.current_term(),
            voter_id: NodeId(9),
            vote_granted: true,
        }),
    });

    assert!(outputs.is_empty());
    assert_eq!(node.role(), Role::Candidate);
}

#[test]
fn follower_rejects_vote_request_when_sender_disagrees_with_candidate_id() {
    let mut node = node(1, &[2, 3]);

    let outputs = node.step(Input::Message {
        from: NodeId(2),
        message: Message::RequestVote(RequestVote {
            term: Term(7),
            candidate_id: NodeId(3),
            last_log_index: LogIndex::ZERO,
            last_log_term: Term::default(),
        }),
    });

    assert!(outputs.is_empty());
    assert_eq!(node.current_term(), Term::default());
    assert_eq!(node.voted_for(), None);
}

#[test]
fn stale_vote_request_is_rejected() {
    let mut node = node(1, &[2, 3]);
    node.become_follower(Term(4));

    let outputs = node.step(Input::Message {
        from: NodeId(2),
        message: Message::RequestVote(RequestVote {
            term: Term(3),
            candidate_id: NodeId(2),
            last_log_index: LogIndex(0),
            last_log_term: Term(0),
        }),
    });

    assert_vote_response(&outputs, NodeId(2), false);
    assert_eq!(node.current_term(), Term(4));
}

#[test]
fn public_transitions_do_not_decrease_current_term() {
    let mut node = node(1, &[2, 3]);
    let _ = node.step(Input::Message {
        from: NodeId(2),
        message: Message::RequestVote(RequestVote {
            term: Term(5),
            candidate_id: NodeId(2),
            last_log_index: LogIndex::ZERO,
            last_log_term: Term::default(),
        }),
    });

    let stale_messages = [
        Message::RequestVote(RequestVote {
            term: Term(4),
            candidate_id: NodeId(3),
            last_log_index: LogIndex::ZERO,
            last_log_term: Term::default(),
        }),
        Message::RequestVoteResponse(RequestVoteResponse {
            term: Term(4),
            voter_id: NodeId(3),
            vote_granted: true,
        }),
        Message::AppendEntries(AppendEntries {
            sequence: 0,
            term: Term(4),
            leader_id: NodeId(3),
            prev_log_index: LogIndex::ZERO,
            prev_log_term: Term::default(),
            entries: Vec::new().into(),
            leader_commit: LogIndex::ZERO,
        }),
        Message::AppendEntriesResponse(AppendEntriesResponse {
            sequence: 0,
            term: Term(4),
            follower_id: NodeId(3),
            success: true,
            match_index: LogIndex::ZERO,
        }),
    ];

    for message in stale_messages {
        let _ = node.step(Input::Message {
            from: NodeId(3),
            message,
        });
        assert_eq!(node.current_term(), Term(5));
    }
}

#[test]
fn leader_emits_heartbeats_on_tick() {
    let mut node = node(1, &[2, 3]);
    let _ = elect_leader(&mut node);

    let outputs = node.step(Input::Tick);

    assert_eq!(outputs.len(), 2);
    assert_append_entries(&outputs[0], NodeId(2), 0);
    assert_append_entries(&outputs[1], NodeId(3), 0);
}

#[test]
fn leader_coalesces_heartbeats_until_interval() {
    let mut node = Node::new(
        NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 3)
            .expect("test Raft node config is valid")
            .with_heartbeat_interval_ticks(2),
    );
    let _ = elect_leader(&mut node);

    assert!(node.step(Input::Tick).is_empty());
    let outputs = node.step(Input::Tick);

    assert_eq!(node.role(), Role::Leader);
    assert_eq!(outputs.len(), 2);
    assert_append_entries(&outputs[0], NodeId(2), 0);
    assert_append_entries(&outputs[1], NodeId(3), 0);
}

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
        node.election_elapsed = u64::MAX - 1;
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
    node.election_elapsed = u64::MAX;

    let outputs = node.step(Input::Tick);

    assert_eq!(node.role(), Role::Candidate);
    assert_eq!(outputs.len(), 1);
}
