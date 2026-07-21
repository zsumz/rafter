//! Compile-process outcome reconciliation with evidence status.

use crate::{
    evidence::{EvidenceStatus, FailureClassification, ResultBundle},
    verification::AggregateError,
};

pub(super) fn verify_compile_process_outcome(
    bundle: &ResultBundle,
    observed: &crate::evidence::format::process::LabeledProcess,
) -> Result<std::collections::BTreeSet<String>, AggregateError> {
    if observed.exit_code == Some(0) && !observed.timed_out {
        return Ok(std::collections::BTreeSet::new());
    }
    let check_prefix = format!("tests/{}#", observed.label);
    let execution_ids = bundle
        .execution
        .checks
        .iter()
        .filter(|check| bundle.runner != "tests" || check.check_id.starts_with(&check_prefix))
        .map(|check| check.execution_id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    if execution_ids.is_empty()
        || bundle.results.iter().any(|result| {
            execution_ids.contains(result.execution_id.as_str())
                && (result.status != EvidenceStatus::Error
                    || result.classification != Some(FailureClassification::HarnessError))
        })
    {
        return Err(AggregateError::new(
            "failed compile process is not reflected as a harness error for every affected check"
                .to_owned(),
        ));
    }
    Ok(execution_ids.into_iter().map(str::to_owned).collect())
}
