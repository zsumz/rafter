use crate::{
    model_check::{catalog, FailureKind},
    SimSeed,
};

use super::super::driver::soak_liveness_round_budget;
use super::{
    leader::{
        run_quorum_only_leader_convergence_check, run_quorum_only_leader_liveness_check,
        run_quorum_only_leader_usability_check,
    },
    production_monitor_state,
};
use crate::model_check::SoakConfig;
use rafter_invariant_test::{oracle_assert, oracle_assert_eq, oracle_expect_err};

#[::rafter_invariant_test::detector_test]
fn quorum_only_leader_monitor_reports_starved_schedule_bound() {
    let config = SoakConfig::new(SimSeed(0xfa17), 0);
    let failure = oracle_expect_err!(
        run_quorum_only_leader_convergence_check(config, 0),
        "zero fair-schedule rounds cannot elect a leader",
    );

    oracle_assert_eq!(
        failure.failure.invariant(),
        catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE
    );
    oracle_assert_eq!(failure.failure.kind(), FailureKind::InvariantViolation);
    oracle_assert!(failure
        .failure
        .message()
        .contains("within 0 fair-schedule rounds"));
}

#[::rafter_invariant_test::detector_test]
fn quorum_only_leader_usability_monitor_reports_exhausted_bound() {
    let config = SoakConfig::new(SimSeed(0xfa17), 0);
    let state = production_monitor_state(config, catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE)
        .expect("production fixture should be valid");
    let convergence_budget = soak_liveness_round_budget(&state, config);
    let failure = oracle_expect_err!(
        run_quorum_only_leader_usability_check(config, convergence_budget, 0),
        "a converged leader cannot complete a fresh proposal in zero rounds",
    );

    oracle_assert_eq!(
        failure.failure.invariant(),
        catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE
    );
    oracle_assert_eq!(failure.failure.kind(), FailureKind::InvariantViolation);
    oracle_assert!(failure
        .failure
        .message()
        .contains("usability probe within 0 fair-schedule rounds"));
    oracle_assert!(failure
        .trace
        .iter()
        .any(|action| matches!(action, crate::model_check::soak::SoakAction::Propose { .. })));
}

#[test]
fn quorum_only_leader_monitor_elects_and_serves_with_minority_unavailable() {
    let config = SoakConfig::new(SimSeed(0xfa17), 0);
    let state = production_monitor_state(config, catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE)
        .expect("production fixture should be valid");
    let budget = soak_liveness_round_budget(&state, config);
    let reports = run_quorum_only_leader_liveness_check(config, budget)
        .expect("reachable quorum should elect a stable usable leader");
    let convergence = reports
        .iter()
        .find(|report| report.feature_id == "quorum-only-leader-convergence")
        .expect("clause a should emit convergence evidence");
    let usability = reports
        .iter()
        .find(|report| report.feature_id == "quorum-only-leader-usability")
        .expect("clause b should emit usability evidence");

    oracle_assert_eq!(convergence.clause_ids, &["LV-01.a"]);
    oracle_assert!(convergence.rounds_used <= convergence.round_limit);
    oracle_assert!(convergence.proposal.is_none());
    oracle_assert_eq!(usability.clause_ids, &["LV-01.b"]);
    oracle_assert!(usability.rounds_used <= usability.round_limit);
    oracle_assert!(usability.stable_leader.is_some_and(|evidence| {
        evidence.stable_rounds >= 2 && evidence.remained_leader_through_probe
    }));
    oracle_assert_eq!(
        usability.proposal.map(|proposal| proposal.outcome.as_str()),
        Some("committed")
    );
    for report in reports {
        oracle_assert!(
            report.validate_structure().is_ok(),
            "production monitor should emit an exact derived bound"
        );
    }
}
