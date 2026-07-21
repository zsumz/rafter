//! Independent acceptance of exact libtest execution outcomes.

use std::path::Path;

use crate::{
    evidence::{CheckReceipt, ResultBundle},
    verification::AggregateError,
};

use super::{
    invocation::{require_unique_discovery, verify_test_process_plan},
    policy::{classify_exact_execution, ExactTestExecution},
};

pub(in crate::artifact_verify) fn verify_test_invocations(
    bundle: &ResultBundle,
    check: &CheckReceipt,
    source: &str,
    test_name: &str,
    oracle_check_id: &str,
    root: &Path,
) -> Result<(), AggregateError> {
    let processes =
        verify_test_process_plan(bundle, check, source, test_name, oracle_check_id, root)?;
    if processes.len() != 3
        || processes
            .iter()
            .any(|process| process.exit_code != Some(0) || process.timed_out)
    {
        return Err(AggregateError::new(
            "test log contains an unsuccessful process invocation".to_owned(),
        ));
    }
    require_unique_discovery(&processes, test_name)?;
    let Some(exact) = processes.last() else {
        return Err(AggregateError::new(
            "test process plan omitted the exact invocation".to_owned(),
        ));
    };
    let executed_test_name = &exact.invocation.arguments[0];
    if classify_exact_execution(
        exact.stdout.as_bytes(),
        exact.stderr.as_bytes(),
        executed_test_name,
        &crate::evidence::format::libtest::oracle_token(&bundle.source_ref, oracle_check_id),
        exact.exit_code,
        exact.timed_out,
    ) != ExactTestExecution::Pass
    {
        return Err(AggregateError::new(
            "test log does not prove one strict exact pass".to_owned(),
        ));
    }
    Ok(())
}

pub(in crate::artifact_verify) fn verify_oracle_failure_invocations(
    bundle: &ResultBundle,
    check: &CheckReceipt,
    source: &str,
    test_name: &str,
    root: &Path,
) -> Result<(), AggregateError> {
    let processes =
        verify_test_process_plan(bundle, check, source, test_name, &check.check_id, root)?;
    let [listed, ignored, exact] = processes.as_slice() else {
        unreachable!("test process plan verifies exact cardinality");
    };
    if listed.exit_code != Some(0)
        || listed.timed_out
        || ignored.exit_code != Some(0)
        || ignored.timed_out
        || exact.exit_code != Some(101)
        || exact.timed_out
    {
        return Err(AggregateError::new(
            "oracle failure receipt must contain two successful discoveries and one clean libtest failure"
                .to_owned(),
        ));
    }
    require_unique_discovery(&processes, test_name)?;
    let executed_test_name = &exact.invocation.arguments[0];
    if classify_exact_execution(
        exact.stdout.as_bytes(),
        exact.stderr.as_bytes(),
        executed_test_name,
        &crate::evidence::format::libtest::oracle_token(&bundle.source_ref, &check.check_id),
        exact.exit_code,
        exact.timed_out,
    ) != ExactTestExecution::InvariantViolation
    {
        return Err(AggregateError::new(
            "oracle failure transcript does not prove one strict exact rejection".to_owned(),
        ));
    }
    Ok(())
}

pub(in crate::artifact_verify) fn verify_incomplete_test_invocations(
    bundle: &ResultBundle,
    check: &CheckReceipt,
    source: &str,
    test_name: &str,
    root: &Path,
) -> Result<(), AggregateError> {
    let processes =
        verify_test_process_plan(bundle, check, source, test_name, &check.check_id, root)?;
    let discovery_success = processes
        .iter()
        .take(2)
        .all(|process| process.exit_code == Some(0) && !process.timed_out);
    if !discovery_success {
        return Err(AggregateError::new(
            "coverage-not-reached transcript contains a failed discovery".to_owned(),
        ));
    }
    match processes.as_slice() {
        [listed, _ignored] => {
            let matches = crate::evidence::format::libtest::listed_tests(listed.stdout.as_bytes())
                .into_iter()
                .filter(|test| test.as_str() == test_name)
                .count();
            if matches != 0 {
                return Err(AggregateError::new(
                    "coverage-not-reached discovery transcript contains the exact test".to_owned(),
                ));
            }
        }
        [_, _, exact] => {
            let executed_test_name = &exact.invocation.arguments[0];
            if classify_exact_execution(
                exact.stdout.as_bytes(),
                exact.stderr.as_bytes(),
                executed_test_name,
                &crate::evidence::format::libtest::oracle_token(
                    &bundle.source_ref,
                    &check.check_id,
                ),
                exact.exit_code,
                exact.timed_out,
            ) != ExactTestExecution::CoverageNotReached
            {
                return Err(AggregateError::new(
                    "coverage-not-reached transcript does not prove zero exact executions"
                        .to_owned(),
                ));
            }
            require_unique_discovery(&processes, test_name)?;
        }
        _ => {
            return Err(AggregateError::new(
                "coverage-not-reached transcript does not prove zero exact executions".to_owned(),
            ))
        }
    }
    Ok(())
}

pub(in crate::artifact_verify) fn verify_harness_error_test_invocations(
    bundle: &ResultBundle,
    check: &CheckReceipt,
    source: &str,
    test_name: &str,
    oracle_check_id: &str,
    root: &Path,
) -> Result<(), AggregateError> {
    let processes =
        verify_test_process_plan(bundle, check, source, test_name, oracle_check_id, root)?;
    match processes.as_slice() {
        [listed, ignored] => {
            let discovery_failed = [listed, ignored]
                .into_iter()
                .any(|process| process.exit_code != Some(0) || process.timed_out);
            let listed_matches =
                crate::evidence::format::libtest::listed_tests(listed.stdout.as_bytes())
                    .into_iter()
                    .filter(|test| test.as_str() == test_name)
                    .count();
            let ignored_matches =
                crate::evidence::format::libtest::listed_tests(ignored.stdout.as_bytes())
                    .into_iter()
                    .filter(|test| test.as_str() == test_name)
                    .count();
            if !discovery_failed && listed_matches <= 1 && ignored_matches <= 1 {
                return Err(AggregateError::new(
                    "harness-error discovery transcript contains no discovery failure or duplicate identity"
                        .to_owned(),
                ));
            }
        }
        [listed, ignored, exact] => {
            require_unique_discovery(&processes, test_name)?;
            let executed_test_name = &exact.invocation.arguments[0];
            if [listed, ignored]
                .into_iter()
                .any(|process| process.exit_code != Some(0) || process.timed_out)
                || classify_exact_execution(
                    exact.stdout.as_bytes(),
                    exact.stderr.as_bytes(),
                    executed_test_name,
                    &crate::evidence::format::libtest::oracle_token(
                        &bundle.source_ref,
                        oracle_check_id,
                    ),
                    exact.exit_code,
                    exact.timed_out,
                ) != ExactTestExecution::HarnessError
            {
                return Err(AggregateError::new(
                    "harness-error execution transcript does not prove an abnormal exact run"
                        .to_owned(),
                ));
            }
        }
        _ => unreachable!("test process plan has two or three invocations"),
    }
    Ok(())
}

pub(in crate::artifact_verify) fn require_exact_test_pass(
    source: &str,
    test_name: &str,
    check_id: &str,
) -> Result<(), AggregateError> {
    let full_result = format!("test {test_name} ... ok");
    let exact_result = format!("::{test_name} ... ok");
    if !source.lines().any(|line| line.trim() == "running 1 test")
        || !source
            .lines()
            .any(|line| line.trim() == full_result || line.trim_end().ends_with(&exact_result))
        || !source
            .lines()
            .any(|line| line.contains("1 passed; 0 failed; 0 ignored"))
    {
        return Err(AggregateError::new(format!(
            "test log does not prove one exact pass for {check_id}"
        )));
    }
    Ok(())
}

pub(in crate::artifact_verify) fn require_exact_test_failure(
    source: &str,
    test_name: &str,
    check_id: &str,
) -> Result<(), AggregateError> {
    let full_result = format!("test {test_name} ... FAILED");
    let exact_result = format!("::{test_name} ... FAILED");
    if !source.lines().any(|line| line.trim() == "running 1 test")
        || !source
            .lines()
            .any(|line| line.trim() == full_result || line.trim_end().ends_with(&exact_result))
        || !source
            .lines()
            .any(|line| line.contains("0 passed; 1 failed; 0 ignored"))
    {
        return Err(AggregateError::new(format!(
            "test log does not prove one exact oracle failure for {check_id}"
        )));
    }
    Ok(())
}
