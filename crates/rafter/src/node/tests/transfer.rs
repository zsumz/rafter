use super::super::*;
use super::helpers::{elect_leader, node};
use crate::{
    AppendEntriesResponse, ConfigurationId, LocalProposalId, LogEntry, Message, PreVote, ReadId,
    RequestVote, RequestVoteResponse, TimeoutNow,
};

fn leader_with_acknowledged_follower() -> Node {
    let mut leader = node(1, &[2, 3]);
    let _ = elect_leader(&mut leader);
    // Follower 2 acknowledges the leader's initial no-op entry.
    let _ = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            sequence: 0,
            term: leader.current_term(),
            follower_id: NodeId(2),
            success: true,
            match_index: LogIndex(1),
        }),
    });
    leader
}

fn timeout_now_to(outputs: &[Output], target: NodeId) -> Option<TimeoutNow> {
    outputs.iter().find_map(|output| match output {
        Output::Send {
            to,
            message: Message::TimeoutNow(request),
        } if *to == target => Some(*request),
        _ => None,
    })
}

#[test]
fn transfer_to_caught_up_target_sends_timeout_now_immediately() {
    let mut leader = leader_with_acknowledged_follower();

    let outputs = leader.step(Input::TransferLeadership { target: NodeId(2) });

    let request = timeout_now_to(&outputs, NodeId(2)).expect("timeout-now goes to the target");
    assert_eq!(request.term, leader.current_term());
    assert_eq!(request.leader_id, NodeId(1));
}

#[test]
fn transfer_waits_for_lagging_target_to_catch_up() {
    let mut leader = node(1, &[2, 3]);
    let _ = elect_leader(&mut leader);
    leader
        .persistent
        .log
        .push(LogEntry::application(Term(1), b"entry".to_vec()));

    let outputs = leader.step(Input::TransferLeadership { target: NodeId(2) });
    assert!(
        timeout_now_to(&outputs, NodeId(2)).is_none(),
        "a lagging target must first catch up"
    );

    // The acknowledgement that completes catch-up triggers the handoff.
    let outputs = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            sequence: 0,
            term: leader.current_term(),
            follower_id: NodeId(2),
            success: true,
            match_index: LogIndex(2),
        }),
    });
    assert!(timeout_now_to(&outputs, NodeId(2)).is_some());
}

#[test]
fn joint_membership_transfer_to_new_voter_waits_for_catchup_then_hands_off() {
    let old_membership =
        crate::MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], Vec::new())
            .expect("valid old membership");
    let new_membership =
        crate::MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3), NodeId(4)], Vec::new())
            .expect("valid new membership");
    let joint_configuration = ConfigurationEntry::joint(
        ConfigurationId(20),
        crate::JointMembership::new(old_membership, new_membership),
    );
    let mut leader = Node::from_bootstrap(
        NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3), NodeId(4)], 3).expect("valid config"),
        BootstrapState {
            current_term: Term(2),
            voted_for: None,
            commit_index: LogIndex(1),
            committed_configuration: Some(crate::CommittedConfiguration {
                index: LogIndex(1),
                config_id: ConfigurationId(20),
            }),
            snapshot: None,
            log: vec![BootstrapLogEntry::configuration(
                LogIndex(1),
                Term(1),
                joint_configuration,
            )],
        },
    )
    .expect("joint membership bootstraps");
    leader.become_leader();

    let outputs = leader.step(Input::TransferLeadership { target: NodeId(4) });
    assert!(
        outputs
            .iter()
            .all(|output| !matches!(output, Output::LeadershipTransferRejected { .. })),
        "the new-side voter is an eligible transfer target in joint membership"
    );
    assert!(
        timeout_now_to(&outputs, NodeId(4)).is_none(),
        "the target must catch up before receiving TimeoutNow"
    );
    assert!(
        outputs.iter().any(|output| matches!(
            output,
            Output::Send {
                to: NodeId(4),
                message: Message::AppendEntries(_),
            }
        )),
        "starting the transfer should push the committed joint entry to the target"
    );

    let outputs = leader.step(Input::Message {
        from: NodeId(4),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            sequence: 0,
            term: leader.current_term(),
            follower_id: NodeId(4),
            success: true,
            match_index: LogIndex(2),
        }),
    });
    assert!(timeout_now_to(&outputs, NodeId(4)).is_some());
}

#[test]
fn proposals_are_rejected_during_transfer_and_resume_after_expiry() {
    let mut leader = leader_with_acknowledged_follower();
    let _ = leader.step(Input::TransferLeadership { target: NodeId(2) });

    let outputs = leader.step(Input::ClientProposal {
        payload: b"blocked".to_vec(),
    });
    assert!(matches!(
        outputs.as_slice(),
        [Output::RejectProposal {
            proposal_id: None,
            reason: ProposalRejection::LeadershipTransferInProgress { target: NodeId(2) },
        }]
    ));
    let tracked_outputs = leader.step(Input::TrackedClientProposal {
        proposal_id: LocalProposalId(9),
        payload: b"blocked-tracked".to_vec(),
    });
    assert!(matches!(
        tracked_outputs.as_slice(),
        [Output::RejectProposal {
            proposal_id: Some(LocalProposalId(9)),
            reason: ProposalRejection::LeadershipTransferInProgress { target: NodeId(2) },
        }]
    ));

    // The transfer expires after one election timeout of leader ticks.
    for _ in 0..leader.config.election_timeout_ticks() {
        let _ = leader.step(Input::Tick);
    }
    let outputs = leader.step(Input::ClientProposal {
        payload: b"accepted".to_vec(),
    });
    assert!(
        !outputs
            .iter()
            .any(|output| matches!(output, Output::RejectProposal { .. })),
        "proposals resume once the transfer expires"
    );
}

#[test]
fn duplicate_transfer_requests_are_rejected_while_pending() {
    let mut leader = leader_with_acknowledged_follower();
    let _ = leader.step(Input::TransferLeadership { target: NodeId(2) });

    let outputs = leader.step(Input::TransferLeadership { target: NodeId(3) });
    assert!(matches!(
        outputs.as_slice(),
        [Output::LeadershipTransferRejected {
            target: NodeId(3),
            reason: LeadershipTransferRejection::TransferAlreadyInProgress,
        }]
    ));
}

#[test]
fn transfer_rejections_cover_non_leader_self_and_non_voter() {
    let mut follower = node(2, &[1, 3]);
    let outputs = follower.step(Input::TransferLeadership { target: NodeId(1) });
    assert!(matches!(
        outputs.as_slice(),
        [Output::LeadershipTransferRejected {
            reason: LeadershipTransferRejection::NotLeader,
            ..
        }]
    ));

    let mut leader = leader_with_acknowledged_follower();
    let outputs = leader.step(Input::TransferLeadership { target: NodeId(1) });
    assert!(matches!(
        outputs.as_slice(),
        [Output::LeadershipTransferRejected {
            reason: LeadershipTransferRejection::TargetIsSelf,
            ..
        }]
    ));

    let outputs = leader.step(Input::TransferLeadership { target: NodeId(9) });
    assert!(matches!(
        outputs.as_slice(),
        [Output::LeadershipTransferRejected {
            reason: LeadershipTransferRejection::TargetNotVoter,
            ..
        }]
    ));
}

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
