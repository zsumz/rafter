use super::*;

#[test]
fn committed_prefix_checker_detects_divergent_committed_entries() {
    let mut cluster = two_node_cluster();
    for (node_id, payload) in [(NodeId(1), b"one-a".as_slice()), (NodeId(2), b"one-b")] {
        let mut bootstrap = bootstrap_state(Term(1), &[(1, Term(1), payload)]);
        bootstrap.commit_index = LogIndex(1);
        cluster
            .restart_node_from_bootstrap(node_id, bootstrap)
            .expect("committed divergent seed is valid");
    }

    let failure = check_committed_prefixes(&cluster, &[])
        .expect_err("divergent committed entries must be detected");
    assert_eq!(
        failure.invariant(),
        catalog::LG_04_COMMITTED_PREFIX_STABILITY
    );
    assert!(
        failure.message.contains("committed prefix diverged"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn commit_index_monotonicity_detects_floor_regression() {
    let mut state = ExplorationState::new(one_node_cluster());
    state.commit_floor_by_node.insert(NodeId(1), LogIndex(2));

    let failure = check_commit_index_monotonicity(&state, &[])
        .expect_err("commit index regression must be detected");
    assert_eq!(
        failure.invariant(),
        catalog::CM_01_COMMIT_INDEX_MONOTONICITY_AND_BOUNDS
    );
    assert!(
        failure.message.contains("commit index regressed"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn committed_configuration_monotonicity_detects_regression() {
    let mut state = ExplorationState::new(one_node_cluster());
    state.committed_configuration_floor_by_node.insert(
        NodeId(1),
        Some(CommittedConfiguration {
            index: LogIndex(3),
            config_id: ConfigurationId(3),
        }),
    );

    let failure = check_committed_configuration_monotonicity(&state, &[])
        .expect_err("committed configuration regression must be detected");
    assert_eq!(
        failure.invariant(),
        catalog::MB_04_MONOTONE_CONFIGURATION_TRANSITION_AND_IDENTITY
    );
    assert!(
        failure
            .message
            .contains("committed configuration regressed"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
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

    let failure = check_no_overlapping_uncommitted_configurations_in_bootstrap(
        &cluster,
        NodeId(1),
        &bootstrap,
        &[],
    )
    .expect_err("two uncommitted configurations must violate MB-03");
    assert_eq!(
        failure.invariant(),
        catalog::MB_03_SERIALIZED_CONFIGURATION_CHANGES
    );
    assert!(
        failure
            .message
            .contains("2 uncommitted configuration entries"),
        "unexpected failure message: {}",
        failure.message
    );
}
