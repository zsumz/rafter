//! Proposal-time controls distinguish admission from later commit knowledge.

use super::*;
use crate::model_check::{
    helpers::{deliver_all_in_state, elect_node_one_in_state},
    observations::Observation,
    state::restart_node,
};
use rafter_invariant_test::{oracle_assert, oracle_assert_eq, oracle_expect_err};

fn leader_with_pending_configuration() -> ExplorationState {
    let mut configs = three_node_configs();
    configs.extend([4, 5].map(|id| {
        NodeConfig::new_non_voter(NodeId(id), ids(&[1, 2, 3]), 3)
            .unwrap()
            .with_pre_vote(false)
            .with_check_quorum(false)
    }));
    let mut state = ExplorationState::new(Cluster::new(configs));
    elect_node_one_in_state(&mut state);
    apply_to_state(
        &mut state,
        Operation::AddLearner {
            to: NodeId(1),
            learner_id: NodeId(4),
        },
    );
    assert_eq!(state.cluster().commit_index(NodeId(1)), LogIndex(1));
    assert_eq!(state.cluster().last_log_index(NodeId(1)), LogIndex(2));
    state
}

/// Inject a faulty local admission into a real leader's pending history.
/// The resulting bootstrap is valid: the fault is the proposal's predecessor
/// not being committed when admitted, which the retained shape cannot prove.
fn overlapping_proposal() -> ExplorationState {
    let mut state = leader_with_pending_configuration();
    let before = state.cluster().transition_observation_snapshot();
    let mut after = before.bootstrap_state(NodeId(1));
    after.log.push(BootstrapLogEntry::configuration(
        LogIndex(3),
        before.current_term(NodeId(1)),
        ConfigurationEntry::stable(
            ConfigurationId(2),
            MembershipSet::new(ids(&[1, 2, 3]), ids(&[4, 5])).unwrap(),
        ),
    ));
    state.inject_bootstrap_state(NodeId(1), after).unwrap();
    state.record_configuration_proposal(&before, NodeId(1));
    state
}

#[::rafter_invariant_test::detector_test]
fn serialized_configuration_checker_detects_proposal_before_predecessor_commit() {
    let mut state = overlapping_proposal();
    let witness = state
        .commit_history()
        .configuration_proposals
        .iter()
        .next_back()
        .unwrap();
    oracle_assert_eq!(witness.index, LogIndex(3));
    oracle_assert_eq!(witness.predecessor, Some(LogIndex(2)));
    oracle_assert_eq!(witness.commit_before, LogIndex(1));

    // Later commitment must not erase the earlier admission violation.
    let mut committed = state.cluster().bootstrap_state(NodeId(1));
    committed.commit_index = LogIndex(3);
    state.inject_bootstrap_state(NodeId(1), committed).unwrap();
    oracle_assert_eq!(state.cluster().commit_index(NodeId(1)), LogIndex(3));
    let failure = oracle_expect_err!(
        check_serialized_configuration_proposals(&state, &[]),
        "later commitment must not hide an overlapping local proposal",
    );
    oracle_assert_eq!(
        failure.invariant(),
        catalog::MB_03_SERIALIZED_CONFIGURATION_CHANGES
    );
    oracle_assert!(failure
        .message
        .contains("before predecessor 2 was committed"));
}

#[test]
fn serialized_configuration_proposal_witnesses_follow_real_admission() {
    let mut state = leader_with_pending_configuration();
    let pending = state.commit_history().configuration_proposals.clone();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending.iter().next().unwrap().predecessor, None);
    assert!(!state
        .observation_set()
        .contains(Observation::ConfigurationProposalPredecessorChecks));
    apply_to_state(
        &mut state,
        Operation::AddLearner {
            to: NodeId(1),
            learner_id: NodeId(5),
        },
    );
    assert_eq!(state.commit_history().configuration_proposals, pending);
    assert_eq!(state.cluster().last_log_index(NodeId(1)), LogIndex(2));

    deliver_all_in_state(&mut state);
    assert_eq!(state.cluster().commit_index(NodeId(1)), LogIndex(2));
    apply_to_state(
        &mut state,
        Operation::AddLearner {
            to: NodeId(1),
            learner_id: NodeId(5),
        },
    );
    let witness = state
        .commit_history()
        .configuration_proposals
        .iter()
        .next_back()
        .unwrap();
    assert_eq!(witness.index, LogIndex(3));
    assert_eq!(witness.predecessor, Some(LogIndex(2)));
    assert_eq!(witness.commit_before, LogIndex(2));
    assert!(state
        .observation_set()
        .contains(Observation::ConfigurationProposalPredecessorChecks));
    check_commit_safety(&state, &[])
        .expect("a serialized local proposal passes the composed oracle");
}

#[test]
fn serialized_configuration_proposal_violation_survives_step_and_restart() {
    let mut state = overlapping_proposal();
    apply_to_state(&mut state, Operation::Tick(NodeId(1)));
    restart_node(&mut state, NodeId(1), &[]).unwrap();
    let failure =
        check_commit_safety(&state, &[]).expect_err("restart cannot erase admission history");
    assert_eq!(
        failure.invariant(),
        catalog::MB_03_SERIALIZED_CONFIGURATION_CHANGES
    );
}

#[test]
fn serialized_configuration_proposal_checks_each_append_in_one_operation() {
    let mut state = leader_with_pending_configuration();
    deliver_all_in_state(&mut state);
    let before = state.cluster().transition_observation_snapshot();
    let mut after = before.bootstrap_state(NodeId(1));
    for index in [3, 4] {
        after.log.push(BootstrapLogEntry::configuration(
            LogIndex(index),
            before.current_term(NodeId(1)),
            ConfigurationEntry::stable(
                ConfigurationId(index),
                MembershipSet::new(ids(&[1, 2, 3]), vec![]).unwrap(),
            ),
        ));
    }
    state.inject_bootstrap_state(NodeId(1), after).unwrap();
    state.record_configuration_proposal(&before, NodeId(1));
    let failure = check_serialized_configuration_proposals(&state, &[]).unwrap_err();
    assert!(failure.message.contains("configuration 4"));
    assert!(failure
        .message
        .contains("before predecessor 3 was committed"));
}

#[test]
fn serialized_configuration_proposal_uses_compacted_predecessor() {
    let mut state = ExplorationState::new(Cluster::new(three_node_configs()));
    let (mut snapshot, payload) = test_snapshot(1, 2, 2, 3, b"committed C1");
    snapshot.metadata.committed_configuration = Some(rafter::SnapshotCommittedConfiguration::new(
        Some(CommittedConfiguration {
            index: LogIndex(1),
            config_id: ConfigurationId(1),
        }),
        stable_membership(&[1, 2, 3], &[]),
    ));
    apply_snapshot_bootstrap_seeds(
        &mut state,
        ids(&[1, 2, 3])
            .into_iter()
            .map(|node_id| SnapshotBootstrapSeed {
                node_id,
                snapshot: snapshot.clone(),
                payload: payload.clone(),
                bootstrap: bootstrap_with_snapshot(Term(3), snapshot.clone(), &[]),
            })
            .collect(),
    )
    .unwrap();
    elect_node_one_in_state(&mut state);
    apply_to_state(
        &mut state,
        Operation::AddLearner {
            to: NodeId(1),
            learner_id: NodeId(4),
        },
    );
    let witness = state
        .commit_history()
        .configuration_proposals
        .iter()
        .next()
        .unwrap();
    assert_eq!(witness.predecessor, Some(LogIndex(1)));
    assert_eq!(witness.index, LogIndex(4));
    assert_eq!(witness.commit_before, LogIndex(3));
    check_commit_safety(&state, &[]).expect("compaction preserves predecessor evidence");
}
