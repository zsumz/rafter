//! Exact status, observation, and artifact matrix for test receipts.

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    contract::catalog::EvidenceDescriptor,
    evidence::{
        CheckCompletion, CheckReceipt, EvidenceStatus, FailureClassification, ResultBundle,
    },
};

pub(crate) fn validate(
    bundle: &ResultBundle,
    expected: &BTreeMap<String, &EvidenceDescriptor>,
) -> Result<(), &'static str> {
    let mut required = BTreeMap::<String, BTreeSet<String>>::new();
    for (evidence_id, descriptor) in expected {
        if let Some(identity) = &descriptor.test {
            required
                .entry(identity.check_id())
                .or_default()
                .insert(evidence_id.clone());
        }
    }
    let observed = bundle
        .execution
        .checks
        .iter()
        .map(|check| {
            (
                check.check_id.clone(),
                check.evidence_ids.iter().cloned().collect::<BTreeSet<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    if observed.len() != bundle.execution.checks.len() || observed != required {
        return Err("tests check identities and evidence fanout must exactly match the registry");
    }
    for check in &bundle.execution.checks {
        validate_check(bundle, check)?;
    }
    Ok(())
}

fn validate_check(bundle: &ResultBundle, check: &CheckReceipt) -> Result<(), &'static str> {
    let results = bundle
        .results
        .iter()
        .filter(|result| result.execution_id == check.execution_id)
        .collect::<Vec<_>>();
    let Some(first) = results.first() else {
        return Err("one tests execution must report at least one evidence result");
    };
    if results.iter().any(|result| {
        result.status != first.status || result.classification != first.classification
    }) {
        return Err("one tests execution cannot report conflicting result statuses");
    }
    let observations = |discovered, executed, passed| {
        BTreeMap::from([
            ("discovered".to_owned(), discovered),
            ("executed".to_owned(), executed),
            ("passed".to_owned(), passed),
        ])
    };
    let test_log_count = check
        .artifacts
        .iter()
        .filter(|artifact| artifact.kind == "test-log")
        .count();
    let test_binary_count = check
        .artifacts
        .iter()
        .filter(|artifact| artifact.kind == "test-binary")
        .count();
    let has_compile_log = check
        .artifacts
        .iter()
        .any(|artifact| artifact.kind == "compile-log");
    let valid = match (first.status, first.classification) {
        (EvidenceStatus::Pass, None) => {
            check.completion == CheckCompletion::Completed
                && check.observations == observations(1, 1, 1)
                && test_log_count == 1
                && test_binary_count == 1
                && results.iter().all(|result| result.artifacts.is_empty())
        }
        (EvidenceStatus::Fail, Some(FailureClassification::InvariantViolation)) => {
            check.completion == CheckCompletion::Counterexample
                && check.observations == observations(1, 1, 0)
                && test_log_count == 1
                && test_binary_count == 1
                && results
                    .iter()
                    .all(|result| result.artifacts == check.artifacts)
        }
        (EvidenceStatus::Incomplete, Some(FailureClassification::CoverageNotReached)) => {
            check.completion == CheckCompletion::CoverageNotReached
                && matches!(check.observations.get("discovered"), Some(0 | 1))
                && check.observations.get("executed") == Some(&0)
                && check.observations.get("passed") == Some(&0)
                && check.observations.len() == 3
                && test_log_count == 1
                && test_binary_count == 1
                && results
                    .iter()
                    .all(|result| result.artifacts == check.artifacts)
        }
        (EvidenceStatus::Error, Some(FailureClassification::HarnessError)) => {
            check.completion == CheckCompletion::HarnessError
                && check.observations.contains_key("discovered")
                && check
                    .observations
                    .get("executed")
                    .is_some_and(|value| matches!(*value, 0 | 1))
                && check.observations.get("passed") == Some(&0)
                && check.observations.len() == 3
                && (has_compile_log || (test_log_count == 1 && test_binary_count == 1))
                && results
                    .iter()
                    .all(|result| result.artifacts == check.artifacts)
        }
        _ => false,
    };
    if !valid {
        return Err("tests result, completion, observations, and artifacts disagree");
    }
    Ok(())
}
