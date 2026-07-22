//! Runner-level reconciliation of test receipts and runtime transcripts.

use std::path::Path;

use crate::{
    contract::catalog::Catalog,
    evidence::{EvidenceStatus, FailureClassification, ResultBundle},
    verification::AggregateError,
};

use super::{
    registered_test_name, require_exact_test_failure, require_exact_test_pass,
    verify_harness_error_test_invocations, verify_incomplete_test_invocations,
    verify_oracle_failure_invocations, verify_test_invocations,
};
use crate::verification::AuthenticatedArtifacts;

pub(crate) fn verify_test_logs(
    bundle: &ResultBundle,
    root: &Path,
    source_root: &Path,
    catalog: &Catalog,
    authenticated: &AuthenticatedArtifacts,
    compilation: &super::super::compiler::CompilationEvidence,
) -> Result<(), AggregateError> {
    let bindings = bundle
        .execution
        .checks
        .iter()
        .map(|check| super::registered_test_binding(catalog, check))
        .collect::<Result<std::collections::BTreeSet<_>, _>>()?;
    crate::verification::target::verify_registered_oracle_sources(source_root, &bindings)
        .map_err(AggregateError::new)?;

    for check in &bundle.execution.checks {
        let outcomes = bundle
            .results
            .iter()
            .filter(|result| result.execution_id == check.execution_id)
            .map(|result| (result.status, result.classification))
            .collect::<Vec<_>>();
        let Some(outcome) = outcomes.first().copied() else {
            return Err(AggregateError::new(format!(
                "tests check {} has no evidence result",
                check.check_id
            )));
        };
        if outcomes.iter().any(|candidate| *candidate != outcome) {
            return Err(AggregateError::new(format!(
                "tests check {} has conflicting evidence outcomes",
                check.check_id
            )));
        }
        let test_name = registered_test_name(catalog, check)?;
        let Some(test_log) = check
            .artifacts
            .iter()
            .find(|artifact| artifact.kind == "test-log")
        else {
            if compile_failure_explains_missing_transcript(
                outcome,
                compilation,
                &check.execution_id,
            ) {
                continue;
            }
            return Err(AggregateError::new(format!(
                "tests check {} is missing its runtime transcript",
                check.check_id
            )));
        };
        let source = authenticated.text(test_log)?;
        validate_canonical_test_transcript(source, &test_log.path)?;
        match outcome {
            (EvidenceStatus::Pass, None) => {
                verify_test_invocations(bundle, check, source, &test_name, &check.check_id, root)?;
                require_exact_test_pass(source, &test_name, &check.check_id)?;
            }
            (EvidenceStatus::Fail, Some(FailureClassification::InvariantViolation)) => {
                verify_oracle_failure_invocations(bundle, check, source, &test_name, root)?;
                require_exact_test_failure(source, &test_name, &check.check_id)?;
            }
            (EvidenceStatus::Incomplete, Some(FailureClassification::CoverageNotReached)) => {
                verify_incomplete_test_invocations(bundle, check, source, &test_name, root)?;
            }
            (EvidenceStatus::Error, Some(FailureClassification::HarnessError)) => {
                verify_harness_error_test_invocations(
                    bundle,
                    check,
                    source,
                    &test_name,
                    &check.check_id,
                    root,
                )?;
            }
            _ => {
                return Err(AggregateError::new(format!(
                    "tests check {} has an invalid status/classification pair",
                    check.check_id
                )))
            }
        }
    }
    Ok(())
}

fn validate_canonical_test_transcript(source: &str, path: &str) -> Result<(), AggregateError> {
    crate::evidence::format::process::parse_combined_processes(source)
        .map(|_| ())
        .map_err(|error| {
            AggregateError::new(format!("parse canonical tests-runner log {path}: {error}"))
        })
}

fn compile_failure_explains_missing_transcript(
    outcome: (EvidenceStatus, Option<FailureClassification>),
    compilation: &super::super::compiler::CompilationEvidence,
    execution_id: &str,
) -> bool {
    outcome
        == (
            EvidenceStatus::Error,
            Some(FailureClassification::HarnessError),
        )
        && compilation.failed_for(execution_id)
}

#[cfg(test)]
#[path = "runner_tests.rs"]
mod tests;
