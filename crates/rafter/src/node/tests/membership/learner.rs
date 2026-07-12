//! Learner replication, snapshots, promotion, and quorum exclusion.

use super::support::*;

mod quorum;

#[test]
fn learner_does_not_start_election_and_its_grant_is_uncounted() {
    let mut learner = node_with_configuration(4, &[1, 2, 3], learner_configuration());

    assert!(learner.step(Input::Tick).is_empty());
    assert!(learner.step(Input::Tick).is_empty());
    assert!(learner.step(Input::Tick).is_empty());
    assert_eq!(learner.role(), Role::Follower);
    assert_eq!(learner.current_term(), Term(1));

    let outputs = learner.step(Input::Message {
        from: NodeId(1),
        message: Message::RequestVote(RequestVote {
            term: Term(2),
            candidate_id: NodeId(1),
            last_log_index: LogIndex(1),
            last_log_term: Term(1),
        }),
    });

    // Learners grant on term and log; candidates never count learner votes
    // toward quorum (see learner_grant_does_not_create_quorum).
    assert_vote_response(&outputs, NodeId(1), true);
}

#[test]
fn non_voting_future_learner_accepts_configuration_and_grant_is_uncounted() {
    let mut learner = Node::new(
        NodeConfig::new_non_voter(NodeId(4), vec![NodeId(1), NodeId(2), NodeId(3)], 3)
            .expect("future learner config is valid"),
    );

    assert!(!learner.is_effective_voter(NodeId(4)));
    assert!(!learner.is_effective_learner(NodeId(4)));
    assert!(learner.step(Input::Tick).is_empty());
    assert!(learner.step(Input::Tick).is_empty());
    assert!(learner.step(Input::Tick).is_empty());
    assert_eq!(learner.role(), Role::Follower);
    assert_eq!(learner.current_term(), Term::default());

    let vote = learner.step(Input::Message {
        from: NodeId(1),
        message: Message::RequestVote(RequestVote {
            term: Term(1),
            candidate_id: NodeId(1),
            last_log_index: LogIndex::ZERO,
            last_log_term: Term::default(),
        }),
    });
    // Granted on term and log; a correct candidate counts only voters.
    assert_vote_response(&vote, NodeId(1), true);

    let outputs = learner.step(Input::Message {
        from: NodeId(1),
        message: Message::AppendEntries(AppendEntries {
            sequence: 0,
            term: Term(1),
            leader_id: NodeId(1),
            prev_log_index: LogIndex::ZERO,
            prev_log_term: Term::default(),
            entries: vec![LogEntry::configuration(Term(1), learner_configuration())].into(),
            leader_commit: LogIndex(1),
        }),
    });

    assert_append_entries_response(&outputs, NodeId(1), true, LogIndex(1));
    assert_eq!(learner.commit_index(), LogIndex(1));
    assert!(!learner.is_effective_voter(NodeId(4)));
    assert!(learner.is_effective_learner(NodeId(4)));
    assert_eq!(
        learner.committed_membership(),
        learner_configuration().membership_config()
    );
}

#[test]
fn learner_receives_log_replication_without_counting_for_commit() {
    let mut leader = committed_leader_with_learner_config();

    let outputs = leader.step(Input::ClientProposal {
        payload: b"promote-after-catch-up".to_vec(),
    });

    assert_eq!(leader.last_log_index(), LogIndex(3));
    assert_eq!(
        send_targets(&outputs),
        vec![NodeId(2), NodeId(3), NodeId(4)]
    );
    assert!(outputs
        .iter()
        .all(|output| append_entries_entry_count(output) == Some(2)));

    assert!(acknowledge(&mut leader, NodeId(4), LogIndex(3)).is_empty());
    assert_eq!(leader.commit_index(), LogIndex(2));

    let commit_outputs = acknowledge(&mut leader, NodeId(2), LogIndex(3));

    assert_eq!(leader.commit_index(), LogIndex(3));
    assert_eq!(
        commit_outputs.iter().find_map(|output| match output {
            Output::Apply { index, payload, .. } => Some((*index, payload.as_slice())),
            Output::LocalProposalAppended { .. }
            | Output::LocalProposalDropped { .. }
            | Output::Send { .. }
            | Output::ApplySnapshot { .. }
            | Output::SendSnapshotChunk { .. }
            | Output::StageSnapshotChunk { .. }
            | Output::RejectProposal { .. }
            | Output::LeadershipTransferRejected { .. }
            | Output::ReadIndexGranted { .. }
            | Output::ReadIndexRejected { .. }
            | Output::ReadIndexCanceled { .. } => None,
        }),
        Some((LogIndex(3), &b"promote-after-catch-up"[..]))
    );
}

#[test]
fn newly_added_learner_receives_retained_suffix_from_boundary() {
    let mut leader = node(1, &[2, 3]);
    let _ = elect_leader(&mut leader);
    let expected_configuration = learner_configuration_with_id(ConfigurationId(1));

    let outputs = leader.step(Input::AddLearner {
        learner_id: NodeId(4),
    });

    assert_eq!(leader.last_log_index(), LogIndex(2));
    assert_eq!(leader.commit_index(), LogIndex::ZERO);
    let learner_append = append_entries_to(&outputs, NodeId(4)).expect("learner append exists");
    assert_eq!(learner_append.prev_log_index, LogIndex::ZERO);
    assert_eq!(learner_append.entries.len(), 2);
    assert_eq!(
        learner_append.entries[1].configuration_entry(),
        Some(&expected_configuration)
    );

    assert!(acknowledge(&mut leader, NodeId(4), LogIndex(2)).is_empty());
    assert!(leader
        .leader_replication_progress()
        .iter()
        .any(|progress| progress.follower_id == NodeId(4) && progress.match_index == LogIndex(2)));
}

#[test]
fn learner_receives_snapshot_replication() {
    let (mut leader, source) = leader_with_snapshot_and_learner_suffix();
    leader
        .try_follower_progress_mut(NodeId(4))
        .expect("active follower")
        .next_index = LogIndex(2);

    let outputs = leader.step(Input::Message {
        from: NodeId(4),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            sequence: 0,
            term: leader.current_term(),
            follower_id: NodeId(4),
            success: false,
            match_index: LogIndex::ZERO,
        }),
    });

    assert_eq!(outputs.len(), 1);
    let Output::SendSnapshotChunk { to, chunk } = &outputs[0] else {
        panic!("expected snapshot chunk send");
    };
    assert_eq!(*to, NodeId(4));
    assert_eq!(chunk.leader_id, NodeId(1));
    assert_eq!(chunk.metadata.last_included_index, LogIndex(1));
    assert_eq!(chunk.offset, 0);
    assert!(chunk.done);
    let request = chunk.resolve(&source).expect("source serves the snapshot");
    assert_eq!(request.chunk, b"learner snapshot".to_vec());
}

#[test]
fn learner_promotion_requires_explicit_barrier() {
    let mut leader = committed_leader_with_learner_config();

    let outputs = leader.step(Input::EnterJoint {
        target: membership(&[1, 2, 3, 4]),
        promotion_barriers: Vec::new(),
    });

    assert_eq!(leader.last_log_index(), LogIndex(2));
    assert_eq!(
        outputs,
        vec![Output::RejectProposal {
            proposal_id: None,
            reason: ProposalRejection::Configuration(
                ConfigurationProposalRejection::MissingPromotionBarrier {
                    learner_id: NodeId(4),
                },
            ),
        }]
    );
}

#[test]
fn lagging_learner_cannot_be_promoted_with_barrier_alone() {
    let mut leader = committed_leader_with_learner_config();
    let barrier = leader
        .promotion_barrier(NodeId(4))
        .expect("learner promotion barrier is available");

    let outputs = leader.step(Input::PromoteLearner {
        learner_id: NodeId(4),
        promotion_barrier: barrier,
    });

    assert_eq!(leader.last_log_index(), LogIndex(2));
    assert_eq!(
        outputs,
        vec![Output::RejectProposal {
            proposal_id: None,
            reason: ProposalRejection::Configuration(
                ConfigurationProposalRejection::PromotionBarrierNotReached {
                    learner_id: NodeId(4),
                    required_match_index: LogIndex(2),
                    actual_match_index: LogIndex::ZERO,
                },
            ),
        }]
    );
}

#[test]
fn duplicate_promotion_barriers_are_rejected() {
    let mut leader = committed_leader_with_learner_config();
    let barrier = leader
        .promotion_barrier(NodeId(4))
        .expect("learner promotion barrier is available");

    let outputs = leader.step(Input::EnterJoint {
        target: membership(&[1, 2, 3, 4]),
        promotion_barriers: vec![barrier, barrier],
    });

    assert_eq!(leader.last_log_index(), LogIndex(2));
    assert_eq!(
        outputs,
        vec![Output::RejectProposal {
            proposal_id: None,
            reason: ProposalRejection::Configuration(
                ConfigurationProposalRejection::DuplicatePromotionBarrier {
                    learner_id: NodeId(4),
                },
            ),
        }]
    );
}

#[test]
fn unused_promotion_barriers_are_rejected() {
    let mut leader = committed_leader_with_learner_config();
    let barrier = leader
        .promotion_barrier(NodeId(4))
        .expect("learner promotion barrier is available");

    let outputs = leader.step(Input::EnterJoint {
        target: membership(&[1, 3]),
        promotion_barriers: vec![barrier],
    });

    assert_eq!(leader.last_log_index(), LogIndex(2));
    assert_eq!(
        outputs,
        vec![Output::RejectProposal {
            proposal_id: None,
            reason: ProposalRejection::Configuration(
                ConfigurationProposalRejection::UnusedPromotionBarrier {
                    learner_id: NodeId(4),
                },
            ),
        }]
    );
}

#[test]
fn caught_up_learner_can_enter_joint_configuration_as_voter() {
    let mut leader = committed_leader_with_learner_config();
    let barrier = leader
        .promotion_barrier(NodeId(4))
        .expect("learner promotion barrier is available");
    assert!(acknowledge(&mut leader, NodeId(4), barrier.required_match_index).is_empty());

    let outputs = leader.step(Input::PromoteLearner {
        learner_id: NodeId(4),
        promotion_barrier: barrier,
    });

    assert_eq!(leader.last_log_index(), LogIndex(3));
    assert_eq!(leader.commit_index(), LogIndex(2));
    assert!(matches!(
        leader
            .entry_at(LogIndex(3))
            .and_then(LogEntry::configuration_entry),
        Some(ConfigurationEntry::Joint { .. })
    ));
    assert_eq!(
        send_targets(&outputs),
        vec![NodeId(2), NodeId(3), NodeId(4)]
    );
    assert!(!outputs
        .iter()
        .any(|output| matches!(output, Output::RejectProposal { .. })));
}
