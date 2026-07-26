//! Election start, single-voter leadership, and candidate promotion scenarios.

use super::*;
use rafter_invariant_test::{oracle_assert, oracle_assert_eq};

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

    oracle_assert_eq!(node.role(), Role::Leader);
    oracle_assert_eq!(node.commit_index(), LogIndex(2));
    oracle_assert!(outputs.iter().any(|output| matches!(
        output,
        Output::Apply { index, payload, .. }
            if *index == LogIndex(1) && payload.as_ref() == b"old-entry"
    )));
    oracle_assert!(outputs.iter().all(|output| !matches!(
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

    oracle_assert_eq!(node.role(), Role::Leader);
    oracle_assert!(outputs.iter().any(|output| matches!(
        output,
        Output::Send {
            message: Message::AppendEntries(_),
            ..
        }
    )));
}

/// `Term::MAX` has no successor, and the protocol's whole ordering is by term.
/// Incrementing past it wrapped to `Term(0)` in release builds — the bootstrap
/// sentinel — so a node would have accepted its own history again as newer,
/// while the same increment panicked under `debug_assertions`. A safety
/// property that depends on the build profile is not one.
///
/// Exhaustion now stops elections instead: the node changes no state, stays a
/// follower, and emits nothing. Both campaign entry points are covered,
/// because the pre-vote poll proposes the successor term before the real
/// election ever asks for it.
#[test]
fn term_exhaustion_stops_elections_instead_of_restarting_history() {
    for pre_vote in [false, true] {
        let mut node = Node::new(
            NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 3)
                .expect("test Raft node config is valid")
                .with_pre_vote(pre_vote),
        );
        node.persistent.current_term = Term::MAX;

        for _ in 0..8 {
            oracle_assert!(
                node.step(Input::Tick).is_empty(),
                "an exhausted term emits no campaign traffic"
            );
        }

        oracle_assert_eq!(node.current_term(), Term::MAX, "and never wraps past it");
        oracle_assert_eq!(node.role(), Role::Follower);
        oracle_assert_eq!(node.voted_for(), None, "nothing was persisted either");
    }
}

/// The saturating successor is the other half of the same guarantee: whatever
/// reaches `Term::next` at the maximum, the one thing it must never produce is
/// a term ordered *below* the one it came from.
#[test]
fn the_maximum_term_has_no_successor_below_itself() {
    oracle_assert_eq!(Term::MAX.next(), Term::MAX);
    oracle_assert_eq!(Term::MAX.checked_next(), None);
    oracle_assert!(!Term::MAX.next().is_zero());
    oracle_assert_eq!(Term(4).checked_next(), Some(Term(5)));
}
