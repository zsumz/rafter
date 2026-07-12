use crate::{model_check::catalog, SimSeed};

use super::proposal::run_proposal_termination_liveness_check;
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
    assert!(
        run_proposal_termination_liveness_check(config, 16).is_ok(),
        "accepted proposal should terminate after isolated leader steps down"
    );
}
