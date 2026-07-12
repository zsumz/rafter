use std::hash::{DefaultHasher, Hash, Hasher};

use super::commit_history::{app_entry, bootstrap_with_log, state_with_bootstraps, voter_configs};
use super::*;

#[test]
fn shorter_matching_commit_observation_preserves_canonical_ledger_identity() {
    let mut state = state_with_bootstraps(
        voter_configs(&[1, 2]),
        &[{
            (
                1,
                bootstrap_with_log(
                    Term(2),
                    LogIndex(2),
                    vec![app_entry(1, Term(1), b"one"), app_entry(2, Term(2), b"two")],
                    None,
                ),
            )
        }],
    );
    let before = hash_commit_history(&state);

    state
        .cluster
        .restart_node_from_bootstrap(
            NodeId(2),
            bootstrap_with_log(
                Term(2),
                LogIndex(1),
                vec![app_entry(1, Term(1), b"one")],
                None,
            ),
        )
        .expect("shorter committed bootstrap is valid");
    state.refresh_log_history();
    state.refresh_committed_prefixes();

    assert_eq!(hash_commit_history(&state), before);
    assert_eq!(
        state
            .commit_history
            .committed_prefix
            .as_ref()
            .map(|prefix| prefix.through),
        Some(LogIndex(2))
    );
    check_commit_history(&state, &[]).expect("matching shorter prefix must remain valid");
}

#[test]
fn shorter_commit_mismatch_is_checked_against_canonical_ledger() {
    let state = state_with_bootstraps(
        voter_configs(&[1, 2]),
        &[
            (
                1,
                bootstrap_with_log(
                    Term(2),
                    LogIndex(2),
                    vec![app_entry(1, Term(1), b"one"), app_entry(2, Term(2), b"two")],
                    None,
                ),
            ),
            (
                2,
                bootstrap_with_log(
                    Term(2),
                    LogIndex(1),
                    vec![app_entry(1, Term(1), b"different-one")],
                    None,
                ),
            ),
        ],
    );

    let failure = check_commit_history(&state, &[])
        .expect_err("shorter divergent committed prefix must be rejected");
    assert_eq!(
        failure.invariant(),
        catalog::LG_04_COMMITTED_PREFIX_STABILITY
    );
    assert!(failure.message.contains("at or before 1"));
}

#[test]
fn leader_completeness_checks_prior_term_entry_hidden_by_newer_suffix() {
    let mut state = state_with_bootstraps(
        voter_configs(&[1, 2]),
        &[{
            (
                1,
                bootstrap_with_log(
                    Term(5),
                    LogIndex(2),
                    vec![
                        app_entry(1, Term(3), b"prior"),
                        app_entry(2, Term(5), b"newer-suffix"),
                    ],
                    None,
                ),
            )
        }],
    );
    state.commit_history.committed_in_terms[0] = Term(3);
    let certificate = election_certificate(4, 2, stable_membership(&[1, 2], &[]), &[1, 2]);
    state
        .election_history
        .elected_by_term
        .insert(certificate.term, certificate);

    state.record_leader_completeness_observation();

    let failure = check_commit_history(&state, &[])
        .expect_err("newer suffix must not hide a missing prior-term committed entry");
    assert_eq!(failure.invariant(), catalog::LG_05_LEADER_COMPLETENESS);
    assert!(failure.message.contains("through 1"));
}

#[test]
fn later_commit_does_not_retroactively_invalidate_an_earlier_leader() {
    let mut state = state_with_bootstraps(voter_configs(&[1, 2, 3]), &[]);
    let certificate = election_certificate(18, 3, stable_membership(&[1, 2, 3], &[]), &[2, 3]);
    state
        .election_history
        .elected_by_term
        .insert(certificate.term, certificate);

    state
        .cluster
        .restart_node_from_bootstrap(
            NodeId(1),
            bootstrap_with_log(
                Term(20),
                LogIndex(2),
                vec![
                    app_entry(1, Term(10), b"old-entry"),
                    app_entry(2, Term(20), b"commit-authority"),
                ],
                None,
            ),
        )
        .expect("later committed bootstrap is valid");
    state.commit_history.committed_in_terms = vec![Term(20), Term(20)];
    state.refresh_log_history();
    state.refresh_committed_prefixes();
    state.record_leader_completeness_observation();

    check_commit_history(&state, &[])
        .expect("a term-20 commit cannot invalidate a leader elected in term 18");
}

#[test]
fn committed_prefix_without_commit_term_provenance_is_a_harness_error() {
    let mut state = state_with_bootstraps(voter_configs(&[1, 2]), &[]);
    state
        .cluster
        .restart_node_from_bootstrap(
            NodeId(1),
            bootstrap_with_log(
                Term(3),
                LogIndex(1),
                vec![app_entry(1, Term(2), b"unwitnessed-commit")],
                None,
            ),
        )
        .expect("injected committed bootstrap is valid");
    state.refresh_log_history();
    state.refresh_committed_prefixes();

    let failure = check_commit_history(&state, &[])
        .expect_err("missing commit authority provenance must fail closed");
    assert_eq!(
        failure.kind(),
        crate::model_check::FailureKind::HarnessError
    );
    assert!(failure.message.contains("no commit-authority term witness"));
}

fn hash_commit_history(state: &ExplorationState) -> u64 {
    let mut hasher = DefaultHasher::new();
    state.commit_history.hash(&mut hasher);
    hasher.finish()
}
