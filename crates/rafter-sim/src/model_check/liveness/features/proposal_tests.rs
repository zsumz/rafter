use crate::{
    model_check::{catalog, FailureKind},
    SimSeed,
};

use super::super::driver::soak_liveness_round_budget;
use super::{
    super::driver::ProposalTerminalOutcome,
    production_monitor_state,
    proposal::{
        run_proposal_progress_liveness_check, run_proposal_progress_liveness_detector,
        run_proposal_termination_liveness_check, run_proposal_termination_liveness_detector,
    },
    TerminalRecorderMode,
};
use crate::model_check::SoakConfig;
use rafter_invariant_test::{oracle_assert, oracle_assert_eq, oracle_expect_err};

#[test]
fn proposal_termination_monitor_reports_unreached_authority_loss_antecedent() {
    let config = SoakConfig::new(SimSeed(0x7e12), 0);
    let failure = run_proposal_termination_liveness_check(config, 0, 0)
        .expect_err("zero authority-loss rounds cannot establish authority loss");

    assert_eq!(
        failure.failure.invariant(),
        catalog::LV_02_PROPOSAL_PROGRESS
    );
    assert_eq!(failure.failure.kind(), FailureKind::CoverageNotReached);
    assert!(failure
        .failure
        .message()
        .contains("did not establish authority loss within 0"));
}

#[test]
fn lv_02_proposal_progress_detector_rejects_exhausted_bound() {
    let config = SoakConfig::new(SimSeed(0x7e12), 0);
    let failure = oracle_expect_err!(
        run_proposal_progress_liveness_detector(
            config,
            1,
            TerminalRecorderMode::DropTerminalRecord,
        ),
        "one bounded-fair round is insufficient for the delayed proposal fixture",
    );

    oracle_assert_eq!(
        failure.failure.invariant(),
        catalog::LV_02_PROPOSAL_PROGRESS
    );
    oracle_assert_eq!(failure.failure.kind(), FailureKind::InvariantViolation);
    oracle_assert!(failure
        .failure
        .message()
        .contains("within 1 bounded-fair rounds"));
    oracle_assert!(failure
        .trace
        .iter()
        .any(|action| matches!(action, crate::model_check::soak::SoakAction::Propose { .. })));
    oracle_assert!(failure
        .trace
        .iter()
        .any(|action| matches!(action, crate::model_check::soak::SoakAction::Tick(_))));
}

#[test]
fn proposal_termination_monitor_observes_authority_loss() {
    let config = SoakConfig::new(SimSeed(0x7e12), 0);
    let state = production_monitor_state(config, catalog::LV_02_PROPOSAL_PROGRESS)
        .expect("production fixture should be valid");
    let budget = soak_liveness_round_budget(&state, config);
    let report = run_proposal_termination_liveness_check(config, budget, budget)
        .expect("accepted proposal should terminate after isolated leader steps down");

    oracle_assert_eq!(report.feature_id, "proposal-termination");
    oracle_assert!(report.rounds_used <= report.round_limit);
    oracle_assert_eq!(
        report.proposal.map(|proposal| proposal.outcome),
        Some(ProposalTerminalOutcome::Unknown),
        "local proposal drops are an unknown-outcome boundary"
    );
    oracle_assert!(report.preconditions.faults_stopped);
    oracle_assert!(!report.preconditions.partition_active);
    oracle_assert!(
        report.validate_structure().is_ok(),
        "production monitor should emit an exact derived bound"
    );
}

#[test]
fn proposal_progress_monitor_reports_measured_commit() {
    let config = SoakConfig::new(SimSeed(0x7e12), 0);
    let state = production_monitor_state(config, catalog::LV_02_PROPOSAL_PROGRESS)
        .expect("production fixture should be valid");
    let budget = soak_liveness_round_budget(&state, config);
    let report = run_proposal_progress_liveness_check(config, budget)
        .expect("stable reachable quorum should commit its accepted proposal");

    assert_eq!(report.feature_id, "proposal-progress");
    assert_eq!(
        report.proposal.map(|proposal| proposal.outcome),
        Some(ProposalTerminalOutcome::Committed)
    );
    assert!(report
        .stable_leader
        .is_some_and(|leader| leader.remained_leader_through_probe));
    report
        .validate_structure()
        .expect("production monitor should emit an exact derived bound");
}

#[test]
fn proposal_termination_monitor_rejects_positive_unreached_antecedent() {
    let config = SoakConfig::new(SimSeed(0x7e12), 0);
    let failure = run_proposal_termination_liveness_check(config, 1, 0)
        .expect_err("one bounded-fair round is insufficient to establish authority loss");

    assert_eq!(
        failure.failure.invariant(),
        catalog::LV_02_PROPOSAL_PROGRESS
    );
    assert_eq!(failure.failure.kind(), FailureKind::CoverageNotReached);
    assert!(failure
        .failure
        .message()
        .contains("did not establish authority loss within 1"));
}

#[test]
fn lv_02_proposal_termination_detector_rejects_exhausted_bound() {
    let config = SoakConfig::new(SimSeed(0x7e12), 0);
    let state = production_monitor_state(config, catalog::LV_02_PROPOSAL_PROGRESS)
        .expect("production fixture should be valid");
    let authority_loss_budget = soak_liveness_round_budget(&state, config);
    let failure = oracle_expect_err!(
        run_proposal_termination_liveness_detector(
            config,
            authority_loss_budget,
            1,
            TerminalRecorderMode::DropTerminalRecord,
        ),
        "one bounded-fair round is insufficient for explicit termination",
    );

    oracle_assert_eq!(
        failure.failure.invariant(),
        catalog::LV_02_PROPOSAL_PROGRESS
    );
    oracle_assert_eq!(failure.failure.kind(), FailureKind::InvariantViolation);
    oracle_assert!(failure
        .failure
        .message()
        .contains("did not reach an explicit terminal state within 1"));
    oracle_assert!(failure
        .trace
        .iter()
        .any(|action| matches!(action, crate::model_check::soak::SoakAction::Heal)));
    oracle_assert!(failure
        .trace
        .iter()
        .any(|action| matches!(action, crate::model_check::soak::SoakAction::Tick(_))));
}
