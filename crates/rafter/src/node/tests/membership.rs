use super::super::*;
use super::helpers::{assert_append_entries_response, assert_vote_response, elect_leader, node};
use super::replication_snapshot_support::{snapshot_source, test_snapshot};
use crate::{
    AppendEntries, AppendEntriesResponse, ConfigurationEntry, ConfigurationId, JointMembership,
    LogEntry, MembershipConfig, MembershipSet, PreVote, PreVoteResponse, RequestVote,
    RequestVoteResponse,
};

#[test]
fn node_without_configuration_entries_uses_static_membership() {
    let node = node(1, &[2, 3]);
    let expected = MembershipConfig::stable(
        MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], Vec::new())
            .expect("static membership is valid"),
    );

    assert_eq!(node.effective_membership(), expected);
    assert!(node.is_effective_voter(NodeId(1)));
    assert!(node.is_effective_voter(NodeId(3)));
    assert!(!node.is_effective_voter(NodeId(4)));
}

#[test]
fn uncommitted_joint_configuration_becomes_effective_from_local_log() {
    let mut follower = node(2, &[1, 3, 4]);
    let joint = joint_configuration(ConfigurationId(9));

    let outputs = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::AppendEntries(AppendEntries {
            sequence: 0,
            term: Term(2),
            leader_id: NodeId(1),
            prev_log_index: LogIndex::ZERO,
            prev_log_term: Term::default(),
            entries: vec![LogEntry::configuration(Term(2), joint.clone())],
            leader_commit: LogIndex::ZERO,
        }),
    });

    assert_eq!(follower.commit_index(), LogIndex::ZERO);
    assert_eq!(follower.last_log_index(), LogIndex(1));
    assert_eq!(follower.effective_membership(), joint.membership_config());
    assert!(follower
        .effective_membership()
        .has_quorum([NodeId(1), NodeId(3)]));
    assert!(!follower
        .effective_membership()
        .has_quorum([NodeId(1), NodeId(2)]));
    assert_append_entries_response(&outputs, NodeId(1), true, LogIndex(1));
}

#[test]
fn committed_membership_ignores_uncommitted_configuration_suffix() {
    let mut follower = node(2, &[1, 3, 4]);
    let joint = joint_configuration(ConfigurationId(9));
    let static_membership = follower.committed_membership();

    assert_append_entries_response(
        &follower.step(Input::Message {
            from: NodeId(1),
            message: Message::AppendEntries(AppendEntries {
                sequence: 0,
                term: Term(2),
                leader_id: NodeId(1),
                prev_log_index: LogIndex::ZERO,
                prev_log_term: Term::default(),
                entries: vec![LogEntry::configuration(Term(2), joint.clone())],
                leader_commit: LogIndex::ZERO,
            }),
        }),
        NodeId(1),
        true,
        LogIndex(1),
    );

    assert_eq!(follower.effective_membership(), joint.membership_config());
    assert_eq!(follower.committed_membership(), static_membership);
    assert_eq!(follower.effective_configuration_entry(), Some(joint));
    assert_eq!(follower.committed_configuration_entry(), None);
}

#[test]
fn follower_rejects_second_uncommitted_configuration_entry() {
    let mut follower = node(2, &[1, 3, 4]);
    let joint = joint_configuration(ConfigurationId(9));
    let final_stable = stable_configuration(ConfigurationId(10), &[1, 3, 4]);

    assert_append_entries_response(
        &follower.step(Input::Message {
            from: NodeId(1),
            message: Message::AppendEntries(AppendEntries {
                sequence: 0,
                term: Term(2),
                leader_id: NodeId(1),
                prev_log_index: LogIndex::ZERO,
                prev_log_term: Term::default(),
                entries: vec![LogEntry::configuration(Term(2), joint)],
                leader_commit: LogIndex::ZERO,
            }),
        }),
        NodeId(1),
        true,
        LogIndex(1),
    );

    let outputs = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::AppendEntries(AppendEntries {
            sequence: 0,
            term: Term(2),
            leader_id: NodeId(1),
            prev_log_index: LogIndex(1),
            prev_log_term: Term(2),
            entries: vec![LogEntry::configuration(Term(2), final_stable)],
            leader_commit: LogIndex::ZERO,
        }),
    });

    assert_eq!(follower.last_log_index(), LogIndex(1));
    assert_append_entries_response(&outputs, NodeId(1), false, LogIndex::ZERO);
}

#[test]
fn follower_accepts_next_configuration_when_frame_commits_previous() {
    let mut follower = node(2, &[1, 3, 4]);
    let joint = joint_configuration(ConfigurationId(9));
    let final_stable = stable_configuration(ConfigurationId(10), &[1, 3, 4]);

    assert_append_entries_response(
        &follower.step(Input::Message {
            from: NodeId(1),
            message: Message::AppendEntries(AppendEntries {
                sequence: 0,
                term: Term(2),
                leader_id: NodeId(1),
                prev_log_index: LogIndex::ZERO,
                prev_log_term: Term::default(),
                entries: vec![LogEntry::configuration(Term(2), joint.clone())],
                leader_commit: LogIndex::ZERO,
            }),
        }),
        NodeId(1),
        true,
        LogIndex(1),
    );

    let outputs = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::AppendEntries(AppendEntries {
            sequence: 1,
            term: Term(2),
            leader_id: NodeId(1),
            prev_log_index: LogIndex(1),
            prev_log_term: Term(2),
            entries: vec![LogEntry::configuration(Term(2), final_stable.clone())],
            leader_commit: LogIndex(1),
        }),
    });

    assert_eq!(follower.commit_index(), LogIndex(1));
    assert_eq!(follower.last_log_index(), LogIndex(2));
    assert_eq!(follower.committed_configuration_entry(), Some(joint));
    assert_eq!(follower.effective_configuration_entry(), Some(final_stable));
    assert_append_entries_response(&outputs, NodeId(1), true, LogIndex(2));
}

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
            entries: vec![LogEntry::configuration(Term(1), learner_configuration())],
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
    leader.follower_progress_mut(NodeId(4)).next_index = LogIndex(2);

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

#[test]
fn change_membership_derives_joint_configuration_for_voter_changes() {
    let mut leader = node(1, &[2, 3]);
    let _ = elect_leader(&mut leader);
    let target = membership(&[1, 3]);

    let outputs = leader.step(Input::ChangeMembership {
        target: target.clone(),
        promotion_barriers: Vec::new(),
    });

    assert_eq!(leader.last_log_index(), LogIndex(2));
    let Some(ConfigurationEntry::Joint {
        config_id,
        membership: joint,
    }) = leader
        .entry_at(LogIndex(2))
        .and_then(LogEntry::configuration_entry)
    else {
        panic!("expected derived joint configuration entry");
    };
    assert_eq!(*config_id, ConfigurationId(1));
    assert_eq!(joint.old(), &membership(&[1, 2, 3]));
    assert_eq!(joint.new_membership(), &target);
    assert!(!outputs
        .iter()
        .any(|output| matches!(output, Output::RejectProposal { .. })));
}

#[test]
fn leave_joint_derives_stable_configuration_from_joint_new_side() {
    let joint = joint_configuration(ConfigurationId(7));
    let mut leader = leader_with_log(vec![BootstrapLogEntry::configuration(
        LogIndex(1),
        Term(2),
        joint,
    )]);
    leader.volatile.commit_index = LogIndex(1);
    leader.volatile.applied_index = LogIndex(1);

    let outputs = leader.step(Input::LeaveJoint);

    assert_eq!(leader.last_log_index(), LogIndex(3));
    let Some(ConfigurationEntry::Stable {
        config_id,
        membership: stable,
    }) = leader
        .entry_at(LogIndex(3))
        .and_then(LogEntry::configuration_entry)
    else {
        panic!("expected derived stable configuration entry");
    };
    assert_eq!(*config_id, ConfigurationId(8));
    assert_eq!(stable, &membership(&[1, 3, 4]));
    assert!(!outputs
        .iter()
        .any(|output| matches!(output, Output::RejectProposal { .. })));
}

#[test]
fn vote_request_from_candidate_outside_effective_membership_is_rejected() {
    let mut voter = node_with_configuration(
        2,
        &[1, 3, 4],
        stable_configuration(ConfigurationId(3), &[1, 2, 3]),
    );

    let outputs = voter.step(Input::Message {
        from: NodeId(4),
        message: Message::RequestVote(RequestVote {
            term: Term(2),
            candidate_id: NodeId(4),
            last_log_index: LogIndex(1),
            last_log_term: Term(1),
        }),
    });

    assert_vote_response(&outputs, NodeId(4), false);
    assert_eq!(voter.current_term(), Term(2));
    assert_eq!(voter.voted_for(), None);
}

#[test]
fn pre_vote_from_candidate_outside_effective_membership_is_rejected() {
    let mut voter = node_with_configuration(
        2,
        &[1, 3, 4],
        stable_configuration(ConfigurationId(3), &[1, 2, 3]),
    );

    let outputs = voter.step(Input::Message {
        from: NodeId(4),
        message: Message::PreVote(PreVote {
            term: Term(2),
            candidate_id: NodeId(4),
            last_log_index: LogIndex(1),
            last_log_term: Term(1),
        }),
    });

    assert_pre_vote_response(&outputs, NodeId(4), Term(1), false);
    assert_eq!(voter.current_term(), Term(1));
    assert_eq!(voter.voted_for(), None);
}

#[test]
fn removed_candidate_steps_down_instead_of_winning() {
    let mut candidate = node_with_configuration(
        2,
        &[1, 3, 4],
        stable_configuration(ConfigurationId(4), &[1, 3, 4]),
    );
    candidate.volatile.role = Role::Candidate;
    candidate.persistent.current_term = Term(2);
    candidate.persistent.voted_for = Some(NodeId(2));
    candidate.granted_votes.insert(NodeId(2));

    let outputs = candidate.step(Input::Message {
        from: NodeId(1),
        message: Message::RequestVoteResponse(RequestVoteResponse {
            term: Term(2),
            voter_id: NodeId(1),
            vote_granted: true,
        }),
    });

    assert!(outputs.is_empty());
    assert_eq!(candidate.role(), Role::Follower);
    assert_eq!(candidate.voted_for(), Some(NodeId(2)));
}

#[test]
fn stable_configuration_entry_commits_with_stable_majority() {
    let mut leader = leader_with_log(vec![BootstrapLogEntry::configuration(
        LogIndex(1),
        Term(2),
        stable_configuration(ConfigurationId(5), &[1, 2, 3]),
    )]);

    assert_eq!(leader.commit_index(), LogIndex::ZERO);

    let outputs = acknowledge(&mut leader, NodeId(2), LogIndex(1));

    assert_eq!(leader.commit_index(), LogIndex(1));
    assert!(outputs
        .iter()
        .all(|output| !matches!(output, Output::Apply { .. })));
}

#[test]
fn joint_configuration_entry_rejects_old_only_and_new_only_majorities() {
    let mut old_only = leader_with_log(vec![BootstrapLogEntry::configuration(
        LogIndex(1),
        Term(2),
        joint_configuration(ConfigurationId(6)),
    )]);

    let outputs = acknowledge(&mut old_only, NodeId(2), LogIndex(1));
    assert!(outputs
        .iter()
        .all(|output| !matches!(output, Output::Apply { .. })));
    assert_eq!(old_only.commit_index(), LogIndex::ZERO);

    let mut new_only = leader_with_log(vec![BootstrapLogEntry::configuration(
        LogIndex(1),
        Term(2),
        joint_configuration(ConfigurationId(6)),
    )]);

    let outputs = acknowledge(&mut new_only, NodeId(4), LogIndex(1));
    assert!(outputs
        .iter()
        .all(|output| !matches!(output, Output::Apply { .. })));
    assert_eq!(new_only.commit_index(), LogIndex::ZERO);

    let mut joint_quorum = leader_with_log(vec![BootstrapLogEntry::configuration(
        LogIndex(1),
        Term(2),
        joint_configuration(ConfigurationId(6)),
    )]);

    let outputs = acknowledge(&mut joint_quorum, NodeId(3), LogIndex(1));

    assert_eq!(joint_quorum.commit_index(), LogIndex(1));
    assert!(outputs
        .iter()
        .all(|output| !matches!(output, Output::Apply { .. })));

    let heartbeat_outputs = joint_quorum.step(Input::Tick);
    let heartbeat = append_entries_to(&heartbeat_outputs, NodeId(3)).expect("commit heartbeat");
    assert_eq!(heartbeat.leader_commit, LogIndex(1));
}

#[test]
fn joint_configuration_governs_application_suffix_commit() {
    let mut old_only = leader_with_log(vec![
        BootstrapLogEntry::configuration(
            LogIndex(1),
            Term(2),
            joint_configuration(ConfigurationId(7)),
        ),
        BootstrapLogEntry::application(LogIndex(2), Term(2), b"joint-governed".to_vec()),
    ]);

    let outputs = acknowledge(&mut old_only, NodeId(2), LogIndex(2));
    assert!(outputs
        .iter()
        .all(|output| !matches!(output, Output::Apply { .. })));
    assert_eq!(old_only.commit_index(), LogIndex::ZERO);

    let mut new_only = leader_with_log(vec![
        BootstrapLogEntry::configuration(
            LogIndex(1),
            Term(2),
            joint_configuration(ConfigurationId(7)),
        ),
        BootstrapLogEntry::application(LogIndex(2), Term(2), b"joint-governed".to_vec()),
    ]);

    let outputs = acknowledge(&mut new_only, NodeId(4), LogIndex(2));
    assert!(outputs
        .iter()
        .all(|output| !matches!(output, Output::Apply { .. })));
    assert_eq!(new_only.commit_index(), LogIndex::ZERO);

    let mut joint_quorum = leader_with_log(vec![
        BootstrapLogEntry::configuration(
            LogIndex(1),
            Term(2),
            joint_configuration(ConfigurationId(7)),
        ),
        BootstrapLogEntry::application(LogIndex(2), Term(2), b"joint-governed".to_vec()),
    ]);

    let outputs = acknowledge(&mut joint_quorum, NodeId(3), LogIndex(2));

    assert_eq!(joint_quorum.commit_index(), LogIndex(2));
    assert_eq!(
        outputs.iter().find_map(|output| match output {
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
        Some((LogIndex(2), &b"joint-governed"[..]))
    );
}

#[test]
fn final_stable_configuration_commits_with_new_majority_after_joint_commit() {
    let mut leader = leader_with_log(vec![BootstrapLogEntry::configuration(
        LogIndex(1),
        Term(2),
        joint_configuration(ConfigurationId(8)),
    )]);
    leader.volatile.commit_index = LogIndex(1);
    leader.volatile.applied_index = LogIndex(1);
    let leave_outputs = leader.step(Input::LeaveJoint);

    assert_eq!(leader.last_log_index(), LogIndex(3));
    assert_eq!(
        leader
            .entry_at(LogIndex(3))
            .and_then(LogEntry::configuration_entry),
        Some(&stable_configuration(ConfigurationId(9), &[1, 3, 4]))
    );
    assert!(!leave_outputs
        .iter()
        .any(|output| matches!(output, Output::RejectProposal { .. })));

    assert!(acknowledge(&mut leader, NodeId(2), LogIndex(3)).is_empty());
    assert_eq!(leader.commit_index(), LogIndex(1));

    let outputs = acknowledge(&mut leader, NodeId(4), LogIndex(3));

    assert_eq!(leader.commit_index(), LogIndex(3));
    assert!(outputs.is_empty());

    let heartbeat_outputs = leader.step(Input::Tick);
    let heartbeat = append_entries_to(&heartbeat_outputs, NodeId(3)).expect("commit heartbeat");
    assert_eq!(heartbeat.leader_commit, LogIndex(3));
}

#[test]
fn grow_then_lose_originals() {
    let mut node = node_with_configuration(
        1,
        &[2, 3],
        stable_configuration(ConfigurationId(3), &[1, 2, 3, 4, 5]),
    );

    assert!(node.step(Input::Tick).is_empty());
    assert!(node.step(Input::Tick).is_empty());
    let outputs = node.step(Input::Tick);

    assert_eq!(node.role(), Role::PreCandidate);
    assert_eq!(
        send_targets(&outputs),
        vec![NodeId(2), NodeId(3), NodeId(4), NodeId(5)]
    );
    assert!(outputs.iter().all(|output| matches!(
        output,
        Output::Send {
            message: Message::PreVote(_),
            ..
        }
    )));

    assert!(grant_pre_vote(&mut node, NodeId(4)).is_empty());
    assert_eq!(node.role(), Role::PreCandidate);
    let outputs = grant_pre_vote(&mut node, NodeId(5));

    assert_eq!(node.role(), Role::Candidate);
    assert_eq!(
        send_targets(&outputs),
        vec![NodeId(2), NodeId(3), NodeId(4), NodeId(5)]
    );
    assert!(outputs.iter().all(|output| matches!(
        output,
        Output::Send {
            message: Message::RequestVote(_),
            ..
        }
    )));

    assert!(grant_vote(&mut node, NodeId(4)).is_empty());
    assert_eq!(node.role(), Role::Candidate);

    let heartbeats = grant_vote(&mut node, NodeId(5));

    assert_eq!(node.role(), Role::Leader);
    assert_eq!(
        send_targets(&heartbeats),
        vec![NodeId(2), NodeId(3), NodeId(4), NodeId(5)]
    );
}

#[test]
fn promoted_voter_grants_vote_only_after_local_membership_includes_it() {
    let request = RequestVote {
        term: Term(2),
        candidate_id: NodeId(4),
        last_log_index: LogIndex(1),
        last_log_term: Term(1),
    };

    let mut current = node_with_configuration(
        2,
        &[1, 3, 4],
        stable_configuration(ConfigurationId(3), &[1, 2, 3, 4]),
    );
    let outputs = current.step(Input::Message {
        from: NodeId(4),
        message: Message::RequestVote(request),
    });
    assert_vote_response(&outputs, NodeId(4), true);

    let mut stale = node_with_configuration(3, &[1, 2, 4], learner_configuration());
    let outputs = stale.step(Input::Message {
        from: NodeId(4),
        message: Message::RequestVote(request),
    });
    assert_vote_response(&outputs, NodeId(4), false);

    let mut candidate = node_with_configuration(
        4,
        &[1, 2, 3],
        stable_configuration(ConfigurationId(3), &[1, 2, 3, 4]),
    );
    assert!(candidate.step(Input::Tick).is_empty());
    assert!(candidate.step(Input::Tick).is_empty());
    let polls = candidate.step(Input::Tick);
    assert_eq!(candidate.role(), Role::PreCandidate);
    assert_eq!(send_targets(&polls), vec![NodeId(1), NodeId(2), NodeId(3)]);

    assert!(grant_pre_vote(&mut candidate, NodeId(2)).is_empty());
    let requests = grant_pre_vote(&mut candidate, NodeId(3));
    assert_eq!(candidate.role(), Role::Candidate);
    assert_eq!(
        send_targets(&requests),
        vec![NodeId(1), NodeId(2), NodeId(3)]
    );

    assert!(grant_vote(&mut candidate, NodeId(2)).is_empty());
    assert_eq!(candidate.role(), Role::Candidate);

    let heartbeats = grant_vote(&mut candidate, NodeId(3));

    assert_eq!(candidate.role(), Role::Leader);
    assert!(!heartbeats.is_empty());
}

#[test]
fn learner_grant_does_not_create_quorum() {
    let mut candidate = node_with_configuration(1, &[2, 3, 4], learner_configuration());

    assert!(candidate.step(Input::Tick).is_empty());
    assert!(candidate.step(Input::Tick).is_empty());
    let polls = candidate.step(Input::Tick);

    assert_eq!(candidate.role(), Role::PreCandidate);
    assert_eq!(send_targets(&polls), vec![NodeId(2), NodeId(3)]);

    // The learner's poll grant creates no pre-vote quorum either.
    assert!(grant_pre_vote(&mut candidate, NodeId(4)).is_empty());
    assert_eq!(candidate.role(), Role::PreCandidate);

    let requests = grant_pre_vote(&mut candidate, NodeId(2));
    assert_eq!(candidate.role(), Role::Candidate);
    assert_eq!(send_targets(&requests), vec![NodeId(2), NodeId(3)]);

    assert!(grant_vote(&mut candidate, NodeId(4)).is_empty());
    assert_eq!(candidate.role(), Role::Candidate);

    let heartbeats = grant_vote(&mut candidate, NodeId(2));

    assert_eq!(candidate.role(), Role::Leader);
    assert!(!heartbeats.is_empty());
}

fn joint_configuration(config_id: ConfigurationId) -> ConfigurationEntry {
    let old = membership(&[1, 2, 3]);
    let new = membership(&[1, 3, 4]);
    ConfigurationEntry::joint(config_id, JointMembership::new(old, new))
}

fn learner_configuration() -> ConfigurationEntry {
    learner_configuration_with_id(ConfigurationId(2))
}

fn learner_configuration_with_id(config_id: ConfigurationId) -> ConfigurationEntry {
    ConfigurationEntry::stable(
        config_id,
        MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], vec![NodeId(4)])
            .expect("learner membership is valid"),
    )
}

fn stable_configuration(config_id: ConfigurationId, voters: &[u64]) -> ConfigurationEntry {
    ConfigurationEntry::stable(config_id, membership(voters))
}

fn committed_leader_with_learner_config() -> Node {
    let mut leader = leader_with_log(vec![BootstrapLogEntry::configuration(
        LogIndex(1),
        Term(2),
        learner_configuration(),
    )]);
    let committed = leader.last_log_index();
    leader.volatile.commit_index = committed;
    leader.volatile.applied_index = committed;
    leader
}

fn leader_with_snapshot_and_learner_suffix() -> (Node, crate::InMemorySnapshotChunkSource) {
    let snapshot = test_snapshot(1, 1, 2, b"learner snapshot");
    let source = snapshot_source(&snapshot, b"learner snapshot".to_vec());
    let mut leader = Node::from_bootstrap(
        NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3), NodeId(4)], 3)
            .expect("test Raft node config is valid"),
        BootstrapState {
            current_term: Term(2),
            voted_for: None,
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
            snapshot: Some(snapshot),
            log: vec![BootstrapLogEntry::configuration(
                LogIndex(2),
                Term(2),
                learner_configuration(),
            )],
        },
    )
    .expect("leader log bootstraps");
    leader.become_leader();
    (leader, source)
}

fn node_with_configuration(id: u64, peers: &[u64], configuration: ConfigurationEntry) -> Node {
    Node::from_bootstrap(
        NodeConfig::new(NodeId(id), peers.iter().copied().map(NodeId).collect(), 3)
            .expect("test Raft node config is valid"),
        BootstrapState {
            current_term: Term(1),
            voted_for: None,
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
            snapshot: None,
            log: vec![BootstrapLogEntry::configuration(
                LogIndex(1),
                Term(1),
                configuration,
            )],
        },
    )
    .expect("configured node bootstraps")
}

fn leader_with_log(log: Vec<BootstrapLogEntry>) -> Node {
    let mut leader = Node::from_bootstrap(
        NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3), NodeId(4)], 3)
            .expect("test Raft node config is valid"),
        BootstrapState {
            current_term: Term(2),
            voted_for: None,
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
            snapshot: None,
            log,
        },
    )
    .expect("leader log bootstraps");
    leader.become_leader();
    leader
}

fn acknowledge(leader: &mut Node, follower_id: NodeId, match_index: LogIndex) -> Vec<Output> {
    leader.step(Input::Message {
        from: follower_id,
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            sequence: 0,
            term: leader.current_term(),
            follower_id,
            success: true,
            match_index,
        }),
    })
}

fn assert_pre_vote_response(outputs: &[Output], to: NodeId, term: Term, vote_granted: bool) {
    assert_eq!(outputs.len(), 1);
    let Output::Send {
        to: actual_to,
        message,
    } = &outputs[0]
    else {
        panic!("expected pre-vote response");
    };
    assert_eq!(*actual_to, to);
    let Message::PreVoteResponse(response) = message else {
        panic!("expected pre-vote response");
    };
    assert_eq!(response.term, term);
    assert_eq!(response.vote_granted, vote_granted);
}

fn membership(voters: &[u64]) -> MembershipSet {
    MembershipSet::new(voters.iter().copied().map(NodeId).collect(), Vec::new())
        .expect("membership is valid")
}

fn send_targets(outputs: &[Output]) -> Vec<NodeId> {
    outputs
        .iter()
        .filter_map(|output| match output {
            Output::Send { to, .. } => Some(*to),
            Output::LocalProposalAppended { .. }
            | Output::LocalProposalDropped { .. }
            | Output::Apply { .. }
            | Output::ApplySnapshot { .. }
            | Output::SendSnapshotChunk { .. }
            | Output::StageSnapshotChunk { .. }
            | Output::RejectProposal { .. }
            | Output::LeadershipTransferRejected { .. }
            | Output::ReadIndexGranted { .. }
            | Output::ReadIndexRejected { .. }
            | Output::ReadIndexCanceled { .. } => None,
        })
        .collect()
}

fn append_entries_entry_count(output: &Output) -> Option<usize> {
    let Output::Send { message, .. } = output else {
        return None;
    };
    let Message::AppendEntries(request) = message else {
        return None;
    };
    Some(request.entries.len())
}

fn append_entries_to(outputs: &[Output], target: NodeId) -> Option<&AppendEntries> {
    outputs.iter().find_map(|output| match output {
        Output::Send {
            to,
            message: Message::AppendEntries(request),
        } if *to == target => Some(request),
        Output::LocalProposalAppended { .. }
        | Output::LocalProposalDropped { .. }
        | Output::Apply { .. }
        | Output::ApplySnapshot { .. }
        | Output::SendSnapshotChunk { .. }
        | Output::StageSnapshotChunk { .. }
        | Output::RejectProposal { .. }
        | Output::LeadershipTransferRejected { .. }
        | Output::ReadIndexGranted { .. }
        | Output::ReadIndexRejected { .. }
        | Output::ReadIndexCanceled { .. }
        | Output::Send { .. } => None,
    })
}

fn grant_vote(node: &mut Node, voter_id: NodeId) -> Vec<Output> {
    node.step(Input::Message {
        from: voter_id,
        message: Message::RequestVoteResponse(RequestVoteResponse {
            term: node.current_term(),
            voter_id,
            vote_granted: true,
        }),
    })
}

/// Grants the pre-candidate's pending poll, which proposes one term past the
/// poller's current term.
fn grant_pre_vote(node: &mut Node, voter_id: NodeId) -> Vec<Output> {
    node.step(Input::Message {
        from: voter_id,
        message: Message::PreVoteResponse(PreVoteResponse {
            term: node.current_term().next(),
            voter_id,
            vote_granted: true,
        }),
    })
}
