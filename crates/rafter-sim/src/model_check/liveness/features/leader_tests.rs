use crate::{model_check::catalog, SimSeed};

use super::leader::run_quorum_only_leader_liveness_check;
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
    assert!(
        run_quorum_only_leader_liveness_check(config, 192).is_ok(),
        "reachable quorum should elect a stable usable leader"
    );
}
