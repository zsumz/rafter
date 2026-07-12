use super::*;

#[test]
fn leader_append_only_detects_leader_term_truncation() {
    let mut cluster = Cluster::new(three_node_configs());
    elect_node_one(&mut cluster);
    let mut state = ExplorationState::new(cluster);
    let leader_id = NodeId(1);
    let leader_term = state.cluster.current_term(leader_id);

    let mut previous = state
        .logical_log_history
        .leader_logs_by_term
        .get(&(leader_id, leader_term))
        .expect("leader observation should be recorded")
        .clone();
    let stale_tail_index = state.cluster.last_log_index(leader_id).next();
    previous.entries.insert(
        stale_tail_index,
        LogEntry::application(leader_term, b"stale-leader-tail".to_vec()),
    );
    state
        .logical_log_history
        .leader_logs_by_term
        .insert((leader_id, leader_term), previous);
    state.refresh_log_history();

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
    let mut before = two_node_cluster();
    before
        .restart_node_from_bootstrap(NodeId(2), bootstrap_state(Term(2), &[(1, Term(1), b"one")]))
        .expect("before follower bootstrap is valid");
    let after = before.clone();
    let delivered = Envelope {
        from: NodeId(1),
        to: NodeId(2),
        message: Message::AppendEntries(request),
    };
    let emitted = [Envelope {
        from: NodeId(2),
        to: NodeId(1),
        message: Message::AppendEntriesResponse(append_success(LogIndex(2))),
    }];
    let mut state = ExplorationState::new(after);
    state.record_log_transition(&before, Some(&delivered), &emitted);

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
    let mut state = ExplorationState::new(cluster);
    state.refresh_log_history();

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

#[test]
fn log_matching_rejects_snapshot_witness_shorter_than_boundary() {
    let (snapshot, payload) = test_snapshot(1, 2, 2, 2, b"snapshot through two");
    let transfer_id = snapshot.transfer_id();
    let mut cluster = one_node_cluster();
    cluster.seed_snapshot_payload(NodeId(1), &snapshot, payload);
    cluster
        .restart_node_from_bootstrap(NodeId(1), bootstrap_with_snapshot(Term(2), snapshot, &[]))
        .expect("snapshot bootstrap is valid");
    let mut state = ExplorationState::new(cluster);
    state
        .logical_log_history
        .snapshot_prefixes_by_owner_transfer
        .insert(
            (NodeId(1), transfer_id),
            LogPrefixWitness {
                through: LogIndex(1),
                entries: vec![LogEntry::application(Term(1), b"one".to_vec())],
            },
        );
    state.refresh_log_history();

    let failure = check_log_history(&state, &[])
        .expect_err("a short witness must not prove a longer snapshot boundary");
    assert_eq!(failure.invariant(), catalog::LG_03_LOG_MATCHING);
    assert!(failure.message.contains("does not match boundary"));
}

#[test]
fn log_matching_rejects_snapshot_witness_with_wrong_boundary_term() {
    let (snapshot, payload) = test_snapshot(1, 2, 2, 2, b"snapshot through two");
    let transfer_id = snapshot.transfer_id();
    let mut cluster = one_node_cluster();
    cluster.seed_snapshot_payload(NodeId(1), &snapshot, payload);
    cluster
        .restart_node_from_bootstrap(NodeId(1), bootstrap_with_snapshot(Term(2), snapshot, &[]))
        .expect("snapshot bootstrap is valid");
    let mut state = ExplorationState::new(cluster);
    state
        .logical_log_history
        .snapshot_prefixes_by_owner_transfer
        .insert(
            (NodeId(1), transfer_id),
            LogPrefixWitness {
                through: LogIndex(2),
                entries: vec![
                    LogEntry::application(Term(1), b"one".to_vec()),
                    LogEntry::application(Term(1), b"wrong-term".to_vec()),
                ],
            },
        );
    state.refresh_log_history();

    let failure = check_log_history(&state, &[])
        .expect_err("a witness with the wrong final term must not prove the boundary");
    assert_eq!(failure.invariant(), catalog::LG_03_LOG_MATCHING);
    assert!(failure.message.contains("does not match boundary"));
}

#[test]
fn log_matching_does_not_bless_unproven_snapshot_from_global_transfer_id() {
    let mut cluster = two_node_cluster();
    cluster
        .restart_node_from_bootstrap(NodeId(1), bootstrap_state(Term(1), &[(1, Term(1), b"one")]))
        .expect("visible source bootstrap is valid");
    let mut state = ExplorationState::new(cluster);
    let (snapshot, payload) = test_snapshot(1, 1, 1, 1, b"snapshot through one");
    for node_id in [NodeId(1), NodeId(2)] {
        state
            .cluster
            .seed_snapshot_payload(node_id, &snapshot, payload.clone());
        state
            .cluster
            .restart_node_from_bootstrap(
                node_id,
                bootstrap_with_snapshot(Term(1), snapshot.clone(), &[]),
            )
            .expect("snapshot bootstrap is valid");
        state.refresh_log_history();
    }

    let failure = check_log_history(&state, &[])
        .expect_err("another owner's transfer ID must not supply snapshot provenance");
    assert_eq!(
        failure.kind(),
        crate::model_check::FailureKind::CoverageNotReached
    );
    assert_eq!(failure.invariant(), catalog::LG_03_LOG_MATCHING);
    assert!(failure.message.contains("has no logical-prefix witness"));
}

#[test]
fn seeded_snapshot_without_prefix_is_coverage_not_reached() {
    let (snapshot, payload) = test_snapshot(1, 2, 1, 2, b"unwitnessed snapshot");
    let mut cluster = one_node_cluster();
    cluster.seed_snapshot_payload(NodeId(1), &snapshot, payload);
    cluster
        .restart_node_from_bootstrap(NodeId(1), bootstrap_with_snapshot(Term(2), snapshot, &[]))
        .expect("snapshot bootstrap is valid");
    let state = ExplorationState::new(cluster);

    let failure = check_log_history(&state, &[])
        .expect_err("a seeded snapshot without prefix provenance cannot pass green");
    assert_eq!(
        failure.kind(),
        crate::model_check::FailureKind::CoverageNotReached
    );
    assert_eq!(failure.invariant(), catalog::LG_03_LOG_MATCHING);
    assert!(
        failure.message.contains("has no logical-prefix witness"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn logical_leader_log_accepts_compaction_with_matching_prefix_witness() {
    let mut cluster = one_node_cluster();
    cluster
        .restart_node_from_bootstrap(
            NodeId(1),
            bootstrap_state(
                Term(2),
                &[
                    (1, Term(1), b"prefix"),
                    (2, Term(2), b"boundary"),
                    (3, Term(2), b"suffix"),
                ],
            ),
        )
        .expect("visible log bootstrap is valid");
    let mut state = ExplorationState::new(cluster);
    let previous = state
        .logical_log_history
        .last_view(NodeId(1))
        .expect("visible view is observed")
        .clone();
    let (snapshot, payload) = test_snapshot(1, 2, 2, 2, b"snapshot through two");
    state
        .cluster
        .seed_snapshot_payload(NodeId(1), &snapshot, payload);
    state
        .cluster
        .restart_node_from_bootstrap(
            NodeId(1),
            bootstrap_with_snapshot(Term(2), snapshot, &[(3, Term(2), b"suffix")]),
        )
        .expect("compacted bootstrap is valid");
    state.refresh_log_history();
    let current = state
        .logical_log_history
        .last_view(NodeId(1))
        .expect("compacted view is observed");

    assert_eq!(
        LogicalLogHistory::observed_log_extends(&previous, current),
        Some(true),
        "matching snapshot prefix must preserve the logical leader log"
    );
    check_log_history(&state, &[]).expect("witnessed compaction must remain green");
}

#[test]
fn leader_append_only_detects_snapshot_only_boundary_regression() {
    let previous = snapshot_only_view(11, &[(1, 1, b"one"), (2, 2, b"two")]);
    let current = snapshot_only_view(12, &[(1, 1, b"one")]);

    assert_eq!(
        LogicalLogHistory::observed_log_extends(&previous, &current),
        Some(false),
        "a snapshot-only leader must not delete its logical suffix"
    );
}

#[test]
fn leader_append_only_detects_snapshot_only_prefix_replacement() {
    let previous = snapshot_only_view(21, &[(1, 1, b"one-a"), (2, 2, b"two")]);
    let current = snapshot_only_view(22, &[(1, 1, b"one-b"), (2, 2, b"two")]);

    assert_eq!(
        LogicalLogHistory::observed_log_extends(&previous, &current),
        Some(false),
        "an equal snapshot boundary must not hide a replaced prefix"
    );
}

#[test]
fn leader_append_only_accepts_higher_snapshot_with_same_logical_prefix() {
    let previous = snapshot_only_view(31, &[(1, 1, b"one"), (2, 2, b"two")]);
    let current = snapshot_only_view(32, &[(1, 1, b"one"), (2, 2, b"two"), (3, 2, b"three")]);

    assert_eq!(
        LogicalLogHistory::observed_log_extends(&previous, &current),
        Some(true),
        "a higher snapshot with the same prior prefix is a legal extension"
    );
}

#[test]
fn leader_append_only_missing_snapshot_witness_is_not_success() {
    let previous = snapshot_only_view(41, &[(1, 1, b"one")]);
    let current = LogicalLogView::snapshot_only(SnapshotTransferId(42), LogIndex(1), Term(1), None);

    assert_eq!(
        LogicalLogHistory::observed_log_extends(&previous, &current),
        None,
        "an unavailable logical prefix must remain coverage-incomplete"
    );
}

fn snapshot_only_view(transfer_sequence: u64, entries: &[(u64, u64, &[u8])]) -> LogicalLogView {
    let entries = entries
        .iter()
        .map(|(_, term, payload)| LogEntry::application(Term(*term), payload.to_vec()))
        .collect::<Vec<_>>();
    let through = LogIndex(entries.len() as u64);
    let term = entries.last().map_or(Term::default(), |entry| entry.term);
    LogicalLogView::snapshot_only(
        SnapshotTransferId(transfer_sequence),
        through,
        term,
        Some(LogPrefixWitness { through, entries }),
    )
}
