//! Process-outcome requirements for scheduled simulator invocations.

use super::super::{verify_simulator_invocation_outcome, AggregateError};

#[test]
fn timed_out_zero_exit_simulator_invocation_is_rejected() {
    let result: Result<(), AggregateError> =
        verify_simulator_invocation_outcome("fast", Some(0), true);
    let error = result.expect_err("timed-out invocation must fail verification");

    assert!(error.to_string().contains("did not time out"));
}
