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

pub(in crate::artifact_verify) fn verify_test_logs(
    bundle: &ResultBundle,
    root: &Path,
    catalog: &Catalog,
    authenticated: &AuthenticatedArtifacts,
) -> Result<(), AggregateError> {
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
            if outcome
                == (
                    EvidenceStatus::Error,
                    Some(FailureClassification::HarnessError),
                )
                && check
                    .artifacts
                    .iter()
                    .any(|artifact| artifact.kind == "compile-log")
            {
                continue;
            }
            return Err(AggregateError::new(format!(
                "tests check {} is missing its runtime transcript",
                check.check_id
            )));
        };
        let source = authenticated.text(test_log)?;
        crate::evidence::format::process::parse_combined_v4(source).map_err(|error| {
            AggregateError::new(format!(
                "parse canonical tests-runner log {}: {error}",
                test_log.path
            ))
        })?;
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
