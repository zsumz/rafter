use crate::{model_check::catalog, SimSeed};

use super::super::driver::soak_liveness_round_budget;
use super::{leader::run_quorum_only_leader_liveness_check, production_monitor_state};
use crate::model_check::SoakConfig;

#[test]
fn quorum_only_leader_monitor_reports_starved_schedule_bound() {
    let config = SoakConfig::new(SimSeed(0xfa17), 0);
    let failure = run_quorum_only_leader_liveness_check(config, 0)
        .expect_err("zero fair-schedule rounds cannot elect a leader");

    assert_eq!(
        failure.failure.invariant(),
        catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE
    );
    assert!(failure
        .failure
        .message()
        .contains("within 0 fair-schedule rounds"));
}

#[test]
fn quorum_only_leader_monitor_elects_and_serves_with_minority_unavailable() {
    let config = SoakConfig::new(SimSeed(0xfa17), 0);
    let state = production_monitor_state(config, catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE)
        .expect("production fixture should be valid");
    let budget = soak_liveness_round_budget(&state, config);
    let report = run_quorum_only_leader_liveness_check(config, budget)
        .expect("reachable quorum should elect a stable usable leader");

    assert_eq!(report.feature_id, "quorum-only-leader-convergence");
    assert!(report.rounds_used <= report.round_limit);
    assert!(report.stable_leader.is_some_and(|evidence| {
        evidence.stable_rounds >= 2 && evidence.remained_leader_through_probe
    }));
    assert_eq!(
        report.proposal.map(|proposal| proposal.outcome.as_str()),
        Some("committed")
    );
    report
        .validate_structure()
        .expect("production monitor should emit an exact derived bound");
}
