use super::*;

#[test]
fn commit_certificate_detects_joint_quorum_missing_new_half() {
    let mut state = ExplorationState::new(one_node_cluster());
    state
        .commit_history
        .violations
        .insert(CommitHistoryViolation {
            invariant: catalog::CM_02_COMMIT_REQUIRES_EFFECTIVE_QUORUM,
            message: "node-1 committed 4 without an effective joint quorum".to_string(),
        });

    let failure =
        check_commit_history(&state, &[]).expect_err("joint quorum missing the new half must fail");
    assert_eq!(
        failure.invariant(),
        catalog::CM_02_COMMIT_REQUIRES_EFFECTIVE_QUORUM
    );
    assert!(
        failure.message.contains("joint quorum"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn commit_certificate_detects_learner_counted_toward_commitment() {
    let mut state = ExplorationState::new(one_node_cluster());
    state
        .commit_history
        .violations
        .insert(CommitHistoryViolation {
            invariant: catalog::CM_02_COMMIT_REQUIRES_EFFECTIVE_QUORUM,
            message: "node-1 committed 5 after counting learner node-4".to_string(),
        });

    let failure = check_commit_history(&state, &[]).expect_err("learner quorum must be rejected");
    assert_eq!(
        failure.invariant(),
        catalog::CM_02_COMMIT_REQUIRES_EFFECTIVE_QUORUM
    );
    assert!(
        failure.message.contains("learner"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn commit_certificate_detects_prior_term_candidate_commit() {
    let mut state = ExplorationState::new(one_node_cluster());
    state
        .commit_history
        .violations
        .insert(CommitHistoryViolation {
            invariant: catalog::CM_03_LEADERS_ONLY_COMMIT_CURRENT_TERM_ENTRIES,
            message: "node-1 advanced commit to 7 for term 2 while leading term 3".to_string(),
        });

    let failure =
        check_commit_history(&state, &[]).expect_err("prior-term candidate commit must fail");
    assert_eq!(
        failure.invariant(),
        catalog::CM_03_LEADERS_ONLY_COMMIT_CURRENT_TERM_ENTRIES
    );
    assert!(
        failure.message.contains("term 2 while leading term 3"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn leader_completeness_detects_later_leader_missing_committed_entry() {
    let mut state = ExplorationState::new(one_node_cluster());
    state
        .commit_history
        .violations
        .insert(CommitHistoryViolation {
            invariant: catalog::LG_05_LEADER_COMPLETENESS,
            message: "node-2 became leader in term 4 without committed prefix through 3"
                .to_string(),
        });

    let failure =
        check_commit_history(&state, &[]).expect_err("missing committed prefix must fail");
    assert_eq!(failure.invariant(), catalog::LG_05_LEADER_COMPLETENESS);
    assert!(
        failure.message.contains("without committed prefix"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn leader_completeness_rejects_unrelated_snapshot_boundary_as_proof() {
    let mut state = ExplorationState::new(one_node_cluster());
    state
        .commit_history
        .violations
        .insert(CommitHistoryViolation {
            invariant: catalog::LG_05_LEADER_COMPLETENESS,
            message: "unrelated snapshot boundary did not cover committed prefix through 6"
                .to_string(),
        });

    let failure = check_commit_history(&state, &[])
        .expect_err("unrelated snapshot boundary must not prove leader completeness");
    assert_eq!(failure.invariant(), catalog::LG_05_LEADER_COMPLETENESS);
    assert!(
        failure.message.contains("snapshot boundary"),
        "unexpected failure message: {}",
        failure.message
    );
}
