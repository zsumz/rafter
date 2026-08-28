//! Tick starvation past a positive fairness bound is a detector rejection.
//!
//! One missed round stays inside a bound of two, but the second missed tick
//! must exhaust that bound and be reported as starvation — tolerating one more
//! round would let a liveness claim rest on a schedule that is not fair.

use super::*;
use rafter_invariant_test::{oracle_assert, oracle_expect_err};

#[::rafter_invariant_test::detector_test]
fn bounded_fairness_detector_rejects_positive_bound_tick_starvation() {
    let mut monitor = BoundedFairnessMonitor::new(2, 3);
    observe_bounded_fairness_round(&mut monitor, &[NodeId(1), NodeId(2)], &[NodeId(1)], 0, 0)
        .expect("one missed round remains inside the positive bound");
    let error = oracle_expect_err!(
        observe_bounded_fairness_round(&mut monitor, &[NodeId(1), NodeId(2)], &[NodeId(1)], 0, 0,),
        "the second missed tick must exhaust the positive bound"
    );

    oracle_assert!(error.contains("tick starvation"));
}
