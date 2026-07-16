use super::super::commit::{
    check_commit_index_monotonicity, check_commit_index_within_local_log_bounds_shape,
    check_committed_configuration_identity, check_committed_configuration_index_monotonicity,
    check_cross_node_committed_prefix_agreement,
    check_no_overlapping_uncommitted_configurations_in_bootstrap,
};
use super::*;
use crate::model_check::observations::Observation;
use rafter_invariant_test::{oracle_assert, oracle_assert_eq, oracle_expect_err};

#[rafter_invariant_test::detector_test]
fn committed_prefix_checker_detects_divergent_committed_entries() {
    let mut cluster = two_node_cluster();
    for (node_id, payload) in [(NodeId(1), b"one-a".as_slice()), (NodeId(2), b"one-b")] {
        let mut bootstrap = bootstrap_state(Term(1), &[(1, Term(1), payload)]);
        bootstrap.commit_index = LogIndex(1);
        cluster
            .restart_node_from_bootstrap(node_id, bootstrap)
            .expect("committed divergent seed is valid");
    }

    let failure = oracle_expect_err!(
        check_cross_node_committed_prefix_agreement(&cluster, &[]),
        "divergent committed entries must be detected",
    );
    oracle_assert_eq!(
        failure.invariant(),
        catalog::LG_04_COMMITTED_PREFIX_STABILITY
    );
    oracle_assert!(
        failure.message.contains("committed prefix diverged"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[rafter_invariant_test::detector_test]
fn commit_index_bound_checker_rejects_commit_beyond_local_last_log() {
    let cluster = one_node_cluster();

    let failure = oracle_expect_err!(
        check_commit_index_within_local_log_bounds_shape(
            &cluster,
            NodeId(1),
            LogIndex(2),
            LogIndex(1),
            &[],
        ),
        "commit beyond local log coverage must be detected",
    );
    oracle_assert_eq!(
        failure.invariant(),
        catalog::CM_01_COMMIT_INDEX_MONOTONICITY_AND_BOUNDS
    );
    oracle_assert!(failure.message.contains("beyond local last log index 1"));
}

#[rafter_invariant_test::detector_test]
fn commit_index_monotonicity_detects_floor_regression() {
    let mut state = ExplorationState::new(one_node_cluster());
    state
        .commit_floor_by_node_mut()
        .insert(NodeId(1), LogIndex(2));

    let failure = oracle_expect_err!(
        check_commit_index_monotonicity(&state, &[]),
        "commit index regression must be detected",
    );
    oracle_assert_eq!(
        failure.invariant(),
        catalog::CM_01_COMMIT_INDEX_MONOTONICITY_AND_BOUNDS
    );
    oracle_assert!(
        failure.message.contains("commit index regressed"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[rafter_invariant_test::detector_test]
fn committed_configuration_monotonicity_detects_regression() {
    let mut state = ExplorationState::new(one_node_cluster());
    state.committed_configuration_floor_by_node_mut().insert(
        NodeId(1),
        Some(CommittedConfiguration {
            index: LogIndex(3),
            config_id: ConfigurationId(3),
        }),
    );

    let failure = oracle_expect_err!(
        check_committed_configuration_index_monotonicity(&state, &[]),
        "committed configuration regression must be detected",
    );
    oracle_assert_eq!(
        failure.invariant(),
        catalog::MB_04_MONOTONE_CONFIGURATION_TRANSITION_AND_IDENTITY
    );
    oracle_assert!(
        failure
            .message
            .contains("committed configuration regressed"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[rafter_invariant_test::detector_test]
fn committed_configuration_identity_detects_same_index_conflict() {
    let mut state = state_with_committed_configuration(ConfigurationId(41));
    state.committed_configuration_floor_by_node_mut().insert(
        NodeId(1),
        Some(CommittedConfiguration {
            index: LogIndex(1),
            config_id: ConfigurationId(42),
        }),
    );

    let failure = oracle_expect_err!(
        check_committed_configuration_identity(&state, &[]),
        "same-index committed configuration identity conflict must be detected",
    );
    oracle_assert_eq!(
        failure.invariant(),
        catalog::MB_04_MONOTONE_CONFIGURATION_TRANSITION_AND_IDENTITY
    );
    oracle_assert!(failure
        .message
        .contains("committed configuration identity changed at index 1"));
}

#[test]
fn commit_bound_and_configuration_identity_observations_require_safe_nonzero_state() {
    let mut state = state_with_committed_configuration(ConfigurationId(41));

    state.refresh_commit_floors();

    assert!(state
        .observation_set()
        .contains(Observation::CommitIndexWithinLocalLogBoundsChecks));
    assert!(state
        .observation_set()
        .contains(Observation::SameIndexCommittedConfigurationIdentityChecks));
}

#[test]
fn lossy_restart_preserves_temporal_commit_and_configuration_floors() {
    let mut state = ExplorationState::new(one_node_cluster());
    let configuration_floor = CommittedConfiguration {
        index: LogIndex(3),
        config_id: ConfigurationId(3),
    };
    state
        .commit_floor_by_node_mut()
        .insert(NodeId(1), LogIndex(2));
    state
        .committed_configuration_floor_by_node_mut()
        .insert(NodeId(1), Some(configuration_floor));

    crate::model_check::state::try_apply_soak_action(
        &mut state,
        crate::model_check::scheduling::SoakOperation::LossyRestart(NodeId(1)),
    )
    .expect("fixture lossy restart must remain valid");

    assert_eq!(state.commit_floor_by_node()[&NodeId(1)], LogIndex(2));
    assert_eq!(
        state.committed_configuration_floor_by_node()[&NodeId(1)],
        Some(configuration_floor)
    );
    assert!(check_commit_index_monotonicity(&state, &[]).is_err());
    assert!(check_committed_configuration_monotonicity(&state, &[]).is_err());
}

#[rafter_invariant_test::detector_test]
fn serialized_configuration_checker_detects_two_uncommitted_configurations() {
    let cluster = one_node_cluster();
    let membership =
        MembershipSet::new(vec![NodeId(1)], Vec::new()).expect("fixture membership is valid");
    let mut bootstrap = bootstrap_state(Term(2), &[]);
    for (index, config_id) in [(1, 41), (2, 42)] {
        bootstrap.log.push(BootstrapLogEntry::configuration(
            LogIndex(index),
            Term(2),
            ConfigurationEntry::stable(ConfigurationId(config_id), membership.clone()),
        ));
    }

    let failure = oracle_expect_err!(
        check_no_overlapping_uncommitted_configurations_in_bootstrap(
            &cluster,
            NodeId(1),
            &bootstrap,
            &[],
        ),
        "two uncommitted configurations must violate MB-03",
    );
    oracle_assert_eq!(
        failure.invariant(),
        catalog::MB_03_SERIALIZED_CONFIGURATION_CHANGES
    );
    oracle_assert!(
        failure
            .message
            .contains("2 uncommitted configuration entries"),
        "unexpected failure message: {}",
        failure.message
    );
}

fn state_with_committed_configuration(config_id: ConfigurationId) -> ExplorationState {
    let mut cluster = one_node_cluster();
    let membership = MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], Vec::new())
        .expect("fixture membership is valid");
    let mut bootstrap = bootstrap_state(Term(2), &[]);
    bootstrap.log.push(BootstrapLogEntry::configuration(
        LogIndex(1),
        Term(2),
        ConfigurationEntry::stable(config_id, membership),
    ));
    bootstrap.commit_index = LogIndex(1);
    bootstrap.committed_configuration = Some(CommittedConfiguration {
        index: LogIndex(1),
        config_id,
    });
    cluster
        .restart_node_from_bootstrap(NodeId(1), bootstrap)
        .expect("committed configuration bootstrap is valid");
    ExplorationState::new(cluster)
}
