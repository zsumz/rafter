//! Exact-process execution, proof qualification, and transcript classification.

use std::{collections::BTreeMap, error::Error, ffi::OsString, path::Path, time::Instant};

use crate::{
    evidence::format::libtest::{exact_failure, exact_pass, exact_zero_execution, oracle_markers},
    execution::filesystem::{HeldDirectory, OperationDeadline, TREE_LIMITS},
};

use super::{detector_policy, detector_proof};
use crate::producer::process;

pub(super) struct ExactProcessExecution {
    pub(super) output: process::ProcessOutput,
    pub(super) detector_challenge: Option<String>,
    pub(super) classification: ExactTestExecution,
    pub(super) harness_error: Option<String>,
}

pub(super) fn run_exact_process(
    program: &str,
    arguments: &[OsString],
    environment: &mut BTreeMap<String, String>,
    test_name: &str,
    oracle_token: &str,
    require_detector_proof: bool,
) -> Result<ExactProcessExecution, Box<dyn Error>> {
    let (output, detector_challenge, harness_error) = if require_detector_proof {
        let detector_proof::Execution {
            output,
            challenge,
            channel_error,
        } = detector_proof::execute(program, arguments, environment)?;
        (
            output,
            Some(challenge),
            channel_error.map(|error| format!("detector proof channel failed: {error}")),
        )
    } else {
        (
            process::timed_for(
                process::ProcessKind::TestExecution,
                program,
                arguments,
                environment,
                Path::new("."),
            )?,
            None,
            None,
        )
    };
    let mut classification = classify_exact_execution(
        &output.stdout,
        &output.stderr,
        test_name,
        oracle_token,
        output.status.code(),
        output.timed_out,
    );
    if harness_error.is_some() {
        classification = ExactTestExecution::HarnessError;
    } else if let Some(challenge) = &detector_challenge {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        let proven =
            detector_policy::classify_transcript(&stdout, &stderr, oracle_token, challenge);
        if !proven.is_ok_and(|witnesses| {
            witnesses
                .keys()
                .any(|witness| witness.starts_with("expect-err:"))
        }) {
            classification = ExactTestExecution::HarnessError;
        }
    }
    Ok(ExactProcessExecution {
        output,
        detector_challenge,
        classification,
        harness_error,
    })
}

pub(in crate::producer) fn reset_test_scratch(
    path: &Path,
    deadline: Instant,
) -> Result<HeldDirectory, Box<dyn Error>> {
    HeldDirectory::replace_tree(
        path,
        TREE_LIMITS,
        OperationDeadline::at(deadline, "test scratch cleanup"),
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExactTestExecution {
    Pass,
    InvariantViolation,
    CoverageNotReached,
    HarnessError,
}

pub(crate) fn classify_exact_execution(
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
    match exit_code {
        Some(0)
            if exact_pass(stdout, test_name)
                && markers.observed == 1
                && markers.violations == 0 =>
        {
            ExactTestExecution::Pass
        }
        Some(0)
            if exact_pass(stdout, test_name)
                && markers.observed == 0
                && markers.violations == 0 =>
        {
            ExactTestExecution::CoverageNotReached
        }
        Some(0)
            if exact_zero_execution(stdout) && markers.observed == 0 && markers.violations == 0 =>
        {
            ExactTestExecution::CoverageNotReached
        }
        Some(101)
            if exact_failure(stdout, test_name)
                && markers.observed <= 1
                && markers.violations == 1 =>
        {
            ExactTestExecution::InvariantViolation
        }
        _ => ExactTestExecution::HarnessError,
    }
}

#[cfg(test)]
mod tests;
