//! Cross-checking between execution receipts and evidence results.

use std::collections::BTreeSet;

use crate::{
    contract::profile::RunnerContract,
    evidence::{CheckCompletion, EvidenceStatus, ResultBundle},
};

use super::structure;

pub(super) fn validate(
    bundle: &ResultBundle,
    runner_contract: &RunnerContract,
) -> Result<(), &'static str> {
    if bundle.execution.checks.len() < runner_contract.minimum_observed_checks {
        return Err("observed check count is below the profile minimum");
    }
    if runner_contract.require_peak_rss && bundle.execution.peak_rss_kib == 0 {
        return Err("peak RSS measurement is missing");
    }
    if bundle.execution.artifacts.is_empty()
        || bundle
            .execution
            .artifacts
            .iter()
            .any(|artifact| !structure::valid_artifact(artifact))
    {
        return Err("execution log artifacts are missing");
    }
    let execution_ids = bundle
        .execution
        .checks
        .iter()
        .map(|check| check.execution_id.as_str())
        .collect::<BTreeSet<_>>();
    if execution_ids.len() != bundle.execution.checks.len()
        || bundle
            .execution
            .checks
            .iter()
            .any(|check| !structure::valid_check(check, runner_contract.require_peak_rss))
    {
        return Err("check receipts must be unique and complete");
    }
    let evaluated = bundle
        .execution
        .checks
        .iter()
        .flat_map(|check| check.evidence_ids.iter())
        .collect::<BTreeSet<_>>();
    let results = bundle
        .results
        .iter()
        .map(|result| &result.evidence_id)
        .collect::<BTreeSet<_>>();
    if results.len() != bundle.results.len() || evaluated != results {
        return Err("evaluated evidence IDs must uniquely match result evidence IDs");
    }
    for result in &bundle.results {
        let Some(check) = bundle
            .execution
            .checks
            .iter()
            .find(|check| check.execution_id == result.execution_id)
        else {
            return Err("result does not reference a check receipt");
        };
        if !check.evidence_ids.contains(&result.evidence_id)
            || !completion_allows(check.completion, result.status)
        {
            return Err("result status disagrees with its check completion");
        }
    }
    Ok(())
}

/// Which result statuses each completion may carry.
///
/// `matches!` gets no exhaustiveness help from the compiler, so a new
/// `CheckCompletion` variant must be added here by hand. It fails closed if it
/// is not -- an unlisted completion allows no status at all -- but the failure
/// arrives as the generic disagreement message, which is a bad way to learn
/// about it. Every variant of the enum appears below.
///
/// `BudgetElapsedFrontierOpen` is a pass. That is the whole point of separating
/// it from `FrontierExhausted`: the verdict semantics of a reporting
/// continuation are unchanged, only the claim it records about its frontier.
fn completion_allows(completion: CheckCompletion, status: EvidenceStatus) -> bool {
    matches!(
        (completion, status),
        (
            CheckCompletion::Completed
                | CheckCompletion::FrontierExhausted
                | CheckCompletion::BudgetElapsedFrontierOpen,
            EvidenceStatus::Pass
        ) | (
            CheckCompletion::Counterexample,
            EvidenceStatus::Fail | EvidenceStatus::Incomplete
        ) | (
            CheckCompletion::CoverageNotReached
                | CheckCompletion::BudgetExhausted
                | CheckCompletion::Timeout,
            EvidenceStatus::Incomplete
        ) | (CheckCompletion::HarnessError, EvidenceStatus::Error)
    )
}
