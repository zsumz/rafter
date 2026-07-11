use super::*;

#[test]
fn leader_append_only_detects_leader_term_truncation() {
    let mut state = ExplorationState::new(one_node_cluster());
    state
        .logical_log_history
        .violations
        .insert(LogicalLogViolation {
            invariant: catalog::LG_01_LEADER_APPEND_ONLY,
            message: "node-1 leader term 4 rewrote or deleted its own log".to_string(),
        });

    let failure =
        check_log_history(&state, &[]).expect_err("leader append-only violation must be reported");
    assert_eq!(failure.invariant(), catalog::LG_01_LEADER_APPEND_ONLY);
    assert!(
        failure.message.contains("rewrote or deleted"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn append_entries_oracle_rejects_success_without_matching_prev() {
    let entry = LogEntry::application(Term(2), b"two".to_vec());
    let request = append_request(Term(9), vec![entry.clone()]);
    let state = append_entries_transition_state(
        &[(1, Term(1), b"one")],
        &[(1, Term(1), b"one"), (2, Term(2), b"two")],
        request,
        append_success(LogIndex(2)),
    );

    let failure = check_log_history(&state, &[])
        .expect_err("success with a mismatched prev term must be detected");
    assert_eq!(
        failure.invariant(),
        catalog::LG_02_TRUTHFUL_APPEND_ENTRIES_ACCEPTANCE
    );
    assert!(
        failure.message.contains("without matching prev"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn append_entries_oracle_detects_success_without_storing_final_entry() {
    let entry = LogEntry::application(Term(2), b"two".to_vec());
    let request = append_request(Term(1), vec![entry]);
    let state = append_entries_transition_state(
        &[(1, Term(1), b"one")],
        &[(1, Term(1), b"one")],
        request,
        append_success(LogIndex(2)),
    );

    let failure = check_log_history(&state, &[])
        .expect_err("success without storing the final entry must be detected");
    assert_eq!(
        failure.invariant(),
        catalog::LG_02_TRUTHFUL_APPEND_ENTRIES_ACCEPTANCE
    );
    assert!(
        failure.message.contains("without storing leader entry"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn append_entries_oracle_detects_inflated_match_index() {
    let entry = LogEntry::application(Term(2), b"two".to_vec());
    let request = append_request(Term(1), vec![entry]);
    let state = append_entries_transition_state(
        &[(1, Term(1), b"one")],
        &[(1, Term(1), b"one"), (2, Term(2), b"two")],
        request,
        append_success(LogIndex(3)),
    );

    let failure =
        check_log_history(&state, &[]).expect_err("inflated success match index must be detected");
    assert_eq!(
        failure.invariant(),
        catalog::LG_02_TRUTHFUL_APPEND_ENTRIES_ACCEPTANCE
    );
    assert!(
        failure.message.contains("reported match index 3"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn log_matching_detects_equal_index_term_with_different_prefixes() {
    let mut cluster = two_node_cluster();
    cluster
        .restart_node_from_bootstrap(
            NodeId(1),
            bootstrap_state(
                Term(2),
                &[(1, Term(1), b"one-a"), (2, Term(2), b"same-term")],
            ),
        )
        .expect("node-1 bootstrap is valid");
    cluster
        .restart_node_from_bootstrap(
            NodeId(2),
            bootstrap_state(
                Term(2),
                &[(1, Term(1), b"one-b"), (2, Term(2), b"same-term")],
            ),
        )
        .expect("node-2 bootstrap is valid");
    let state = ExplorationState::new(cluster);

    let failure = check_log_history(&state, &[])
        .expect_err("log matching must detect equal index/term with different prefixes");
    assert_eq!(failure.invariant(), catalog::LG_03_LOG_MATCHING);
    assert!(
        failure.message.contains("different prefix"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn log_matching_detects_snapshot_boundary_hiding_mismatched_prefix() {
    let mut cluster = two_node_cluster();
    cluster
        .restart_node_from_bootstrap(
            NodeId(2),
            bootstrap_state(
                Term(2),
                &[(1, Term(1), b"one-b"), (2, Term(2), b"same-term")],
            ),
        )
        .expect("node-2 visible bootstrap is valid");
    let mut state = ExplorationState::new(cluster);

    let (snapshot, payload) = test_snapshot(2, 2, 2, 2, b"snapshot through two");
    state
        .cluster
        .seed_snapshot_payload(NodeId(2), &snapshot, payload);
    state
        .cluster
        .restart_node_from_bootstrap(NodeId(2), bootstrap_with_snapshot(Term(2), snapshot, &[]))
        .expect("node-2 compacted bootstrap is valid");
    state.refresh_log_history();

    state
        .cluster
        .restart_node_from_bootstrap(
            NodeId(1),
            bootstrap_state(
                Term(2),
                &[(1, Term(1), b"one-a"), (2, Term(2), b"same-term")],
            ),
        )
        .expect("node-1 visible bootstrap is valid");
    state.refresh_log_history();

    let failure = check_log_history(&state, &[])
        .expect_err("snapshot-hidden prefix mismatch must be reported");
    assert_eq!(failure.invariant(), catalog::LG_03_LOG_MATCHING);
    assert!(
        failure.message.contains("different prefix"),
        "unexpected failure message: {}",
        failure.message
    );
}
