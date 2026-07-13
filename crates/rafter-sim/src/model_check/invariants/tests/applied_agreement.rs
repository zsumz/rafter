use super::*;

#[test]
fn real_transition_persists_recorder_failure_as_a_harness_error() {
    let mut state = ExplorationState::new(one_node_cluster());
    state.remove_execution_cursor(NodeId(1));

    crate::model_check::state::apply_to_state(
        &mut state,
        crate::model_check::scheduling::Operation::Tick(NodeId(1)),
    );
    let cloned = state.clone();

    for state in [&state, &cloned] {
        let failure = check_commit_safety(state, &[])
            .expect_err("a recorder failure must make commit safety red");
        assert_eq!(
            failure.kind(),
            crate::model_check::FailureKind::HarnessError
        );
        assert_eq!(failure.invariant(), catalog::AP_02_STATE_MACHINE_SAFETY);
        assert!(
            failure.message.contains("no execution-history cursor"),
            "unexpected failure message: {}",
            failure.message
        );
    }
}

#[test]
fn execution_agreement_detects_mismatched_configuration_application() {
    let mut state = ExplorationState::new(one_node_cluster());
    for (node, config_id, voters) in [(1, 7, &[1, 2][..]), (2, 8, &[1, 2, 3][..])] {
        let configuration = ConfigurationEntry::stable(
            ConfigurationId(config_id),
            MembershipSet::new(ids(voters), Vec::new()).expect("fixture membership is valid"),
        );
        state.inject_execution_witness(execution_witness(
            node,
            0,
            4,
            2,
            LogEntryKind::Configuration(configuration),
            initial_reference_state(),
        ));
    }

    let failure = check_execution_history_agreement(&state, &[])
        .expect_err("different configurations at one index must fail AP-02");
    assert_eq!(failure.invariant(), catalog::AP_02_STATE_MACHINE_SAFETY);
    assert!(
        failure
            .message
            .contains("different term/kind/input identities at log index 4"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn execution_agreement_detects_mismatched_reference_result() {
    let mut state = ExplorationState::new(one_node_cluster());
    for node in [1, 2] {
        let mut witness = execution_witness(
            node,
            0,
            2,
            1,
            LogEntryKind::Application(b"same-command".to_vec().into()),
            initial_reference_state(),
        );
        if node == 2 {
            witness.resulting_state.application_value = b"broken-result".to_vec().into();
        }
        state.inject_execution_witness(witness);
    }

    let failure = check_execution_history_agreement(&state, &[])
        .expect_err("a fabricated application result must fail AP-02");
    assert_eq!(failure.invariant(), catalog::AP_02_STATE_MACHINE_SAFETY);
    assert!(
        failure
            .message
            .contains("invalid reference-state result at log index 2"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn execution_agreement_detects_broken_configuration_result() {
    let configuration = ConfigurationEntry::stable(
        ConfigurationId(7),
        MembershipSet::new(ids(&[1, 2]), Vec::new()).expect("fixture membership is valid"),
    );
    let mut state = ExplorationState::new(one_node_cluster());
    for node in [1, 2] {
        let mut witness = execution_witness(
            node,
            0,
            4,
            2,
            LogEntryKind::Configuration(configuration.clone()),
            initial_reference_state(),
        );
        if node == 2 {
            witness.resulting_state.committed_membership = stable_membership(&[1, 3], &[]);
        }
        state.inject_execution_witness(witness);
    }

    let failure = check_execution_history_agreement(&state, &[])
        .expect_err("a broken configuration result must fail AP-02");
    assert_eq!(failure.invariant(), catalog::AP_02_STATE_MACHINE_SAFETY);
    assert!(
        failure
            .message
            .contains("invalid reference-state result at log index 4"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn applied_agreement_detects_disagreeing_snapshots_at_same_boundary() {
    let mut cluster = one_node_cluster();
    for (node, payload) in [(1, b"state-a".to_vec()), (2, b"state-b".to_vec())] {
        cluster.snapshot_installs.push(SnapshotInstalled {
            node_id: NodeId(node),
            application_epoch: 0,
            last_included_index: LogIndex(4),
            last_included_term: Term(1),
            committed_membership: None,
            payload,
            applied_records_before_install: 0,
        });
    }

    let failure = check_applied_payload_agreement(&cluster, &[])
        .expect_err("disagreeing snapshots must be detected");
    assert_eq!(
        failure.invariant(),
        catalog::SS_05_SNAPSHOT_SEMANTIC_EQUIVALENCE
    );
    assert!(
        failure.message.contains("disagreeing snapshots at index 4"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn applied_agreement_detects_snapshot_membership_mismatch_at_same_boundary() {
    let mut cluster = one_node_cluster();
    for (node, membership) in [
        (1, stable_membership(&[1, 2, 3], &[])),
        (2, stable_membership(&[1, 2], &[3])),
    ] {
        cluster.snapshot_installs.push(SnapshotInstalled {
            node_id: NodeId(node),
            application_epoch: 0,
            last_included_index: LogIndex(4),
            last_included_term: Term(1),
            committed_membership: Some(membership),
            payload: b"state".to_vec(),
            applied_records_before_install: 0,
        });
    }

    let failure = check_applied_payload_agreement(&cluster, &[])
        .expect_err("same-boundary membership mismatch must be detected");
    assert_eq!(
        failure.invariant(),
        catalog::SS_05_SNAPSHOT_SEMANTIC_EQUIVALENCE
    );
    assert!(
        failure.message.contains("disagreeing snapshots at index 4"),
        "unexpected failure message: {}",
        failure.message
    );
}
