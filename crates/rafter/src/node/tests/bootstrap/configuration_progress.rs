//! Lagging commit knowledge preserves quorum rules and configuration serialization.

use super::support::*;

fn old() -> MembershipSet {
    MembershipSet::new(
        vec![NodeId(1), NodeId(2), NodeId(3)],
        vec![NodeId(4), NodeId(5)],
    )
    .unwrap()
}

fn new() -> MembershipSet {
    MembershipSet::new(vec![NodeId(1), NodeId(4), NodeId(5)], Vec::new()).unwrap()
}

fn recovered(id: u64, joint: bool) -> Node {
    let stable = ConfigurationEntry::stable(ConfigurationId(1), old());
    let second = if joint {
        ConfigurationEntry::stable(ConfigurationId(2), old())
    } else {
        ConfigurationEntry::joint(ConfigurationId(2), JointMembership::new(old(), new()))
    };
    let third = if joint {
        ConfigurationEntry::joint(ConfigurationId(3), JointMembership::new(old(), new()))
    } else {
        ConfigurationEntry::stable(ConfigurationId(3), new())
    };
    Node::from_bootstrap(
        NodeConfig::new(
            NodeId(id),
            (1..=3).filter(|peer| *peer != id).map(NodeId).collect(),
            3,
        )
        .unwrap(),
        BootstrapState {
            current_term: Term(3),
            voted_for: None,
            commit_index: LogIndex(1),
            committed_configuration: Some(CommittedConfiguration {
                index: LogIndex(1),
                config_id: ConfigurationId(1),
            }),
            snapshot: None,
            log: vec![
                BootstrapLogEntry::configuration(LogIndex(1), Term(1), stable),
                BootstrapLogEntry::configuration(LogIndex(2), Term(2), second),
                BootstrapLogEntry::configuration(LogIndex(3), Term(2), third),
            ],
        },
    )
    .expect("accepted catch-up prefix can precede durable commit publication")
}

fn grant(node: &mut Node, voter: u64) {
    let _ = node.step(Input::Message {
        from: NodeId(voter),
        message: Message::RequestVoteResponse(RequestVoteResponse {
            term: node.current_term(),
            voter_id: NodeId(voter),
            vote_granted: true,
        }),
    });
}

fn acknowledge(node: &mut Node, voter: u64) -> Vec<Output> {
    node.step(Input::Message {
        from: NodeId(voter),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            sequence: 0,
            term: node.current_term(),
            follower_id: NodeId(voter),
            success: true,
            match_index: LogIndex(4),
        }),
    })
}

fn assert_proposal_waits(node: &mut Node) {
    let before = node.last_log_index();
    assert!(node
        .step(Input::AddLearner {
            learner_id: NodeId(6)
        })
        .iter()
        .any(|output| matches!(
            output,
            Output::RejectProposal {
                reason: ProposalRejection::Configuration(
                    ConfigurationProposalRejection::UncommittedConfiguration { index: LogIndex(2) }
                ),
                ..
            }
        )));
    assert_eq!(node.last_log_index(), before);
}

#[test]
fn recovered_joint_configuration_requires_both_election_and_commit_majorities() {
    for first in [2, 4] {
        let mut node = recovered(1, true);
        assert_eq!(node.committed_membership(), MembershipConfig::Stable(old()));
        assert_eq!(
            node.effective_membership(),
            MembershipConfig::Joint(JointMembership::new(old(), new()))
        );
        node.start_election();
        grant(&mut node, first);
        assert_eq!(
            node.role(),
            Role::Candidate,
            "one side of the joint quorum cannot elect"
        );
        let second = if first == 2 { 4 } else { 2 };
        grant(&mut node, second);
        assert_eq!(node.role(), Role::Leader);
        assert_eq!(node.commit_index(), LogIndex(1));
        assert_eq!(node.last_log_index(), LogIndex(4));
        assert_proposal_waits(&mut node);
        let _ = node.drain_committed_outputs();
        acknowledge(&mut node, first);
        assert_eq!(
            node.commit_index(),
            LogIndex(1),
            "one side cannot commit the new-term no-op"
        );
        let outputs = acknowledge(&mut node, second);
        assert_eq!(node.commit_index(), LogIndex(4));
        let committed: Vec<_> = outputs
            .iter()
            .filter_map(|output| match output {
                Output::ConfigurationCommitted { index, .. } => Some(*index),
                _ => None,
            })
            .collect();
        assert_eq!(committed, [LogIndex(2), LogIndex(3)]);
        assert!(node
            .step(Input::LeaveJoint)
            .iter()
            .all(|output| !matches!(output, Output::RejectProposal { .. })));
        assert_eq!(
            node.effective_configuration_entry().unwrap().config_id(),
            ConfigurationId(4)
        );
    }
}

#[test]
fn recovered_final_stable_configuration_uses_its_new_majority() {
    let mut node = recovered(1, false);
    node.start_election();
    grant(&mut node, 2);
    assert_eq!(node.role(), Role::Candidate);
    grant(&mut node, 4);
    assert_eq!(node.role(), Role::Leader);
    assert_proposal_waits(&mut node);
    acknowledge(&mut node, 2);
    assert_eq!(node.commit_index(), LogIndex(1));
    acknowledge(&mut node, 4);
    assert_eq!(node.commit_index(), LogIndex(4));
    assert_eq!(node.committed_membership(), MembershipConfig::Stable(new()));
    let _ = node.step(Input::AddLearner {
        learner_id: NodeId(6),
    });
    assert_eq!(
        node.effective_configuration_entry().unwrap().config_id(),
        ConfigurationId(4)
    );
}

#[test]
fn recovered_follower_accepts_noop_but_requires_commit_proof_for_another_configuration() {
    let mut follower = recovered(2, true);
    let input = |entries: Vec<LogEntry>, prev, leader_commit| Input::Message {
        from: NodeId(1),
        message: Message::AppendEntries(AppendEntries {
            sequence: 1,
            term: Term(4),
            leader_id: NodeId(1),
            prev_log_index: LogIndex(prev),
            prev_log_term: Term(if prev == 3 { 2 } else { 4 }),
            entries: entries.into(),
            leader_commit: LogIndex(leader_commit),
        }),
    };
    let outputs = follower.step(input(vec![LogEntry::noop(Term(4))], 3, 1));
    assert_append_entries_response(&outputs, NodeId(1), true, LogIndex(4));
    assert_eq!(follower.commit_index(), LogIndex(1));
    let stable = LogEntry::configuration(
        Term(4),
        ConfigurationEntry::stable(ConfigurationId(4), new()),
    );
    let outputs = follower.step(input(vec![stable.clone()], 4, 1));
    assert_append_entries_response(&outputs, NodeId(1), false, LogIndex::ZERO);
    assert_eq!(follower.last_log_index(), LogIndex(4));
    let outputs = follower.step(input(vec![stable], 4, 3));
    assert!(outputs.iter().any(|output| matches!(output,
        Output::Send { message: Message::AppendEntriesResponse(response), .. }
            if response.success && response.match_index == LogIndex(5)
    )));
    assert_eq!(follower.commit_index(), LogIndex(3));
}

#[test]
fn recovered_leader_catches_up_a_partial_follower_before_relearning_commitment() {
    let mut leader = recovered(1, true);
    leader.start_election();
    grant(&mut leader, 2);
    grant(&mut leader, 4);
    assert_eq!(leader.role(), Role::Leader);
    let mut follower = Node::from_bootstrap(
        NodeConfig::new(NodeId(2), vec![NodeId(1), NodeId(3)], 3).unwrap(),
        BootstrapState {
            current_term: Term(3),
            voted_for: None,
            commit_index: LogIndex(1),
            committed_configuration: Some(CommittedConfiguration {
                index: LogIndex(1),
                config_id: ConfigurationId(1),
            }),
            snapshot: None,
            log: vec![BootstrapLogEntry::configuration(
                LogIndex(1),
                Term(1),
                ConfigurationEntry::stable(ConfigurationId(1), old()),
            )],
        },
    )
    .unwrap();
    let outputs = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::AppendEntries(AppendEntries {
            sequence: 1,
            term: leader.current_term(),
            leader_id: NodeId(1),
            prev_log_index: LogIndex(1),
            prev_log_term: Term(1),
            entries: leader.log_entries_from(LogIndex(2)).into(),
            leader_commit: leader.commit_index(),
        }),
    });
    assert_append_entries_response(&outputs, NodeId(1), true, LogIndex(4));
    assert_eq!(follower.commit_index(), LogIndex(1));
    assert_eq!(
        follower.effective_membership(),
        leader.effective_membership()
    );
    acknowledge(&mut leader, 4);
    assert_eq!(leader.commit_index(), LogIndex(1));
    acknowledge(&mut leader, 2);
    assert_eq!(leader.commit_index(), LogIndex(4));
}
