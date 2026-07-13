use crate::{model_check::catalog, SimSeed};

use super::super::driver::soak_liveness_round_budget;
use super::{
    super::driver::ProposalTerminalOutcome,
    production_monitor_state,
    proposal::{run_proposal_progress_liveness_check, run_proposal_termination_liveness_check},
};
use crate::model_check::SoakConfig;

#[test]
fn proposal_termination_monitor_reports_exhausted_bound() {
    let config = SoakConfig::new(SimSeed(0x7e12), 0);
    let failure = run_proposal_termination_liveness_check(config, 0)
        .expect_err("zero authority-loss rounds cannot reach termination");

    assert_eq!(
        failure.failure.invariant(),
        catalog::LV_02_PROPOSAL_PROGRESS
    );
    assert!(failure
        .failure
        .message()
        .contains("did not reach an explicit terminal state within 0"));
}

#[test]
fn proposal_termination_monitor_observes_authority_loss() {
    let config = SoakConfig::new(SimSeed(0x7e12), 0);
    let state = production_monitor_state(config, catalog::LV_02_PROPOSAL_PROGRESS)
        .expect("production fixture should be valid");
    let budget = soak_liveness_round_budget(&state, config);
    let report = run_proposal_termination_liveness_check(config, budget)
        .expect("accepted proposal should terminate after isolated leader steps down");

    assert_eq!(report.feature_id, "proposal-termination");
    assert!(report.rounds_used <= report.round_limit);
    assert_eq!(
        report.proposal.map(|proposal| proposal.outcome),
        Some(ProposalTerminalOutcome::Unknown),
        "local proposal drops are an unknown-outcome boundary"
    );
    assert!(report.preconditions.faults_stopped);
    assert!(!report.preconditions.partition_active);
    report
        .validate_structure()
        .expect("production monitor should emit an exact derived bound");
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
fn proposal_termination_monitor_rejects_positive_exhausted_bound() {
    let config = SoakConfig::new(SimSeed(0x7e12), 0);
    let failure = run_proposal_termination_liveness_check(config, 1)
        .expect_err("one bounded-fair round is insufficient for authority-loss termination");

    assert_eq!(
        failure.failure.invariant(),
        catalog::LV_02_PROPOSAL_PROGRESS
    );
    assert!(failure.failure.message().contains("within 1"));
}
