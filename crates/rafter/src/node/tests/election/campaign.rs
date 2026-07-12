//! Election start, single-voter leadership, and candidate promotion scenarios.

use super::*;

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
