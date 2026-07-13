//! `TimeoutNow` election, term fencing, learner exclusion, and old-leader step-down.

use super::support::*;

#[test]
fn timeout_now_elects_immediately_even_with_pre_vote_enabled() {
    let mut target = Node::new(
        NodeConfig::new(NodeId(2), vec![NodeId(1), NodeId(3)], 3)
            .expect("valid config")
            .with_pre_vote(true),
    );

    let outputs = target.step(Input::Message {
        from: NodeId(1),
        message: Message::TimeoutNow(TimeoutNow {
            term: Term::default(),
            leader_id: NodeId(1),
        }),
    });

    // A real, term-incrementing election starts at once: no pre-vote round.
    assert_eq!(target.role(), Role::Candidate);
    assert_eq!(target.current_term(), Term(1));
    assert!(outputs.iter().all(|output| !matches!(
        output,
        Output::Send {
            message: Message::PreVote(PreVote { .. }),
            ..
        }
    )));
    assert!(outputs.iter().any(|output| matches!(
        output,
        Output::Send {
            message: Message::RequestVote(RequestVote { .. }),
            ..
        }
    )));
}
#[test]
fn old_leader_steps_down_when_transfer_target_wins() {
    let mut leader = leader_with_acknowledged_follower();
    let _ = leader.step(Input::TransferLeadership { target: NodeId(2) });

    // The target's higher-term vote request deposes the old leader.
    let outputs = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::RequestVote(RequestVote {
            term: leader.current_term().next(),
            candidate_id: NodeId(2),
            last_log_index: leader.last_log_index(),
            last_log_term: Term(1),
        }),
    });

    assert_eq!(leader.role(), Role::Follower);
    super::helpers::assert_vote_response(&outputs, NodeId(2), true);
}
#[test]
fn timeout_now_is_ignored_by_a_learner() {
    // Node 4 is a learner under the committed configuration: TimeoutNow must
    // not make a non-voter campaign.
    let learner_configuration = ConfigurationEntry::stable(
        ConfigurationId(2),
        crate::MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], vec![NodeId(4)])
            .expect("valid membership"),
    );
    let mut learner = Node::from_bootstrap(
        NodeConfig::new(NodeId(4), vec![NodeId(1), NodeId(2), NodeId(3)], 3).expect("valid config"),
        BootstrapState {
            current_term: Term(1),
            voted_for: None,
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
            snapshot: None,
            log: vec![BootstrapLogEntry::configuration(
                LogIndex(1),
                Term(1),
                learner_configuration,
            )],
        },
    )
    .expect("learner bootstraps");

    let outputs = learner.step(Input::Message {
        from: NodeId(1),
        message: Message::TimeoutNow(TimeoutNow {
            term: Term(1),
            leader_id: NodeId(1),
        }),
    });

    assert!(outputs.is_empty());
    assert_eq!(learner.role(), Role::Follower);
    assert_eq!(learner.current_term(), Term(1));
}
#[test]
fn stale_term_timeout_now_is_ignored() {
    let mut node = node(2, &[1, 3]);
    node.persistent.current_term = Term(5);

    let outputs = node.step(Input::Message {
        from: NodeId(1),
        message: Message::TimeoutNow(TimeoutNow {
            term: Term(3),
            leader_id: NodeId(1),
        }),
    });

    assert!(outputs.is_empty());
    assert_eq!(node.role(), Role::Follower);
    assert_eq!(node.current_term(), Term(5));
}
#[test]
fn timeout_now_to_a_stale_leader_sheds_all_leader_state_first() {
    // A leader of an older term receiving TimeoutNow must pass through
    // become_follower so no per-term leader state leaks into its next reign.
    let mut leader = leader_with_acknowledged_follower();
    let _ = leader.step(Input::ClientProposal {
        payload: b"commit-me".to_vec(),
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
    let _ = leader.step(Input::ReadIndex { read_id: ReadId(8) });
    let _ = leader.step(Input::TransferLeadership { target: NodeId(2) });
    assert_eq!(leader.pending_read_count(), 1);

    let old_term = leader.current_term();
    let _ = leader.step(Input::Message {
        from: NodeId(3),
        message: Message::TimeoutNow(TimeoutNow {
            term: old_term.next(),
            leader_id: NodeId(3),
        }),
    });

    assert_eq!(leader.role(), Role::Candidate);
    assert_eq!(leader.current_term(), Term(old_term.next().0 + 1));
    assert_eq!(
        leader.pending_read_count(),
        0,
        "old-term barriers must not survive"
    );

    // Re-winning must not inherit the old transfer: proposals are accepted.
    let _ = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::RequestVoteResponse(RequestVoteResponse {
            term: leader.current_term(),
            voter_id: NodeId(2),
            vote_granted: true,
        }),
    });
    assert_eq!(leader.role(), Role::Leader);
    let outputs = leader.step(Input::ClientProposal {
        payload: b"fresh-term".to_vec(),
    });
    assert!(
        !outputs
            .iter()
            .any(|output| matches!(output, Output::RejectProposal { .. })),
        "a stale pending transfer must not block the new term"
    );
}
