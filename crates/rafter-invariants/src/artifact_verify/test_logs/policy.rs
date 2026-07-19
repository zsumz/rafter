//! Independent verifier policy for exact libtest execution outcomes.

use crate::evidence::format::libtest::{
    exact_failure, exact_pass, exact_zero_execution, oracle_markers,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ExactTestExecution {
    Pass,
    InvariantViolation,
    CoverageNotReached,
    HarnessError,
}

pub(super) fn classify_exact_execution(
    stdout: &[u8],
    stderr: &[u8],
    test_name: &str,
    oracle_token: &str,
    exit_code: Option<i32>,
    timed_out: bool,
) -> ExactTestExecution {
    if timed_out {
        return ExactTestExecution::HarnessError;
    }
    let Some(markers) = oracle_markers(stdout, stderr, oracle_token) else {
        return ExactTestExecution::HarnessError;
    };
    if exit_code == Some(0)
        && exact_pass(stdout, test_name)
        && markers.observed == 1
        && markers.violations == 0
    {
        return ExactTestExecution::Pass;
    }
    if exit_code == Some(0)
        && (exact_pass(stdout, test_name) || exact_zero_execution(stdout))
        && markers.observed == 0
        && markers.violations == 0
    {
        return ExactTestExecution::CoverageNotReached;
    }
    if exit_code == Some(101)
        && exact_failure(stdout, test_name)
        && markers.observed <= 1
        && markers.violations == 1
    {
        return ExactTestExecution::InvariantViolation;
    }
    ExactTestExecution::HarnessError
}
