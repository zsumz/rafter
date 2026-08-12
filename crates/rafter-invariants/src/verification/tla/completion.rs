//! TLA+ completion and counterexample classification.

use crate::{
    evidence::{
        format::{
            process::ProcessLog,
            tla::{checkpoint::RecoveryReport, checkpoint::RecoveryStatus, TlcSummary},
        },
        CheckCompletion, ContinuationOutcome, EvidenceStatus, FailureClassification,
        PrimaryCompletionPolicy, ResultBundle,
    },
    verification::AggregateError,
};

use super::{
    observation::{successful_log, successful_summary},
    source::configuration,
};

pub(super) fn verify_counterexample_binding(
    bundle: &ResultBundle,
    violated: Option<&str>,
) -> Result<(), AggregateError> {
    match violated {
        None if bundle
            .results
            .iter()
            .all(|result| result.status != EvidenceStatus::Fail) =>
        {
            Ok(())
        }
        Some("TypeOK")
            if bundle.results.iter().all(|result| {
                result.status == EvidenceStatus::Error
                    && result.classification == Some(FailureClassification::HarnessError)
            }) =>
        {
            Ok(())
        }
        Some(symbol) => {
            let bound = bundle
                .results
                .iter()
                .filter(|result| evidence_symbol(&result.evidence_id) == Some(symbol))
                .collect::<Vec<_>>();
            if !bound.is_empty()
                && bound
                    .iter()
                    .all(|result| result.status == EvidenceStatus::Fail)
                && bundle.results.iter().all(|result| {
                    (evidence_symbol(&result.evidence_id) == Some(symbol))
                        == (result.status == EvidenceStatus::Fail)
                })
            {
                return Ok(());
            }
            Err(AggregateError::new(
                "TLA counterexample frame does not match the failed evidence result".to_owned(),
            ))
        }
        _ => Err(AggregateError::new(
            "TLA counterexample frame does not match the failed evidence result".to_owned(),
        )),
    }
}

fn evidence_symbol(evidence_id: &str) -> Option<&str> {
    evidence_id.rsplit_once('#').map(|(_, symbol)| symbol)
}

/// Rederives the continuation's declared policy and outcome from the evidence.
///
/// The policy is compared against the pinned contract rather than accepted from
/// the receipt, so a producer cannot demote its own gate; the outcome is
/// recomputed from the same log the verifier already parsed, so a receipt
/// cannot claim its monolith drained when it did not.
pub(super) fn verify_continuation_binding(
    bundle: &ResultBundle,
    policy: PrimaryCompletionPolicy,
    main: Option<&ProcessLog>,
    summary: Option<&TlcSummary>,
) -> Result<(), AggregateError> {
    let binding = bundle
        .execution
        .checks
        .first()
        .and_then(|check| check.tla_continuation)
        .ok_or_else(|| {
            AggregateError::new("TLA receipt omitted its continuation binding".to_owned())
        })?;
    if binding.policy != policy {
        return Err(AggregateError::new(format!(
            "TLA receipt claims {:?} but the profile pins {policy:?}",
            binding.policy
        )));
    }
    let expected = if summary.is_some_and(|summary| summary.violated_invariant.is_some()) {
        ContinuationOutcome::Counterexample
    } else if main.is_some_and(|log| log.timed_out) {
        ContinuationOutcome::BudgetElapsedFrontierOpen
    } else if summary.is_some_and(|summary| summary.states_left == 0) {
        ContinuationOutcome::FrontierExhausted
    } else {
        ContinuationOutcome::BudgetElapsedFrontierOpen
    };
    if binding.outcome != expected {
        return Err(AggregateError::new(format!(
            "TLA receipt reports continuation outcome {:?} but the log shows {expected:?}",
            binding.outcome
        )));
    }
    Ok(())
}

pub(super) fn verify_completion(
    bundle: &ResultBundle,
    trace_passed: bool,
    detectors_passed: bool,
    obligations_passed: bool,
    policy: PrimaryCompletionPolicy,
    checkpoint: Option<&RecoveryReport>,
    main: Option<&ProcessLog>,
    summary: Option<&TlcSummary>,
) -> Result<(), AggregateError> {
    // An undischarged obligation is a harness-level failure of the layer, in
    // the same class as a broken detector: it means the primary run was never
    // entitled to start, so no completion it might have claimed is admissible.
    let expected = if !trace_passed
        || !detectors_passed
        || !obligations_passed
        || checkpoint.is_some_and(|report| report.status == RecoveryStatus::Incompatible)
    {
        CheckCompletion::HarnessError
    } else if let Some(violated) = summary.and_then(|summary| summary.violated_invariant.as_deref())
    {
        if violated == "TypeOK" {
            CheckCompletion::HarnessError
        } else {
            CheckCompletion::Counterexample
        }
    } else if main.is_some_and(|log| log.timed_out) {
        // A reporting continuation is expected to end here. It is only a pass
        // when the progress frame is readable, which `timeout_progress` has
        // already required of the same log.
        if policy.gates() {
            CheckCompletion::Timeout
        } else {
            CheckCompletion::FrontierExhausted
        }
    } else if let (Some(main), Some(summary)) = (main, summary) {
        if successful_log(main) && successful_summary(summary) {
            if !policy.gates() {
                CheckCompletion::FrontierExhausted
            } else {
                let minimum_generated = configuration(bundle, "minimum_generated_states")?
                    .parse::<u64>()
                    .map_err(|_| {
                        AggregateError::new("invalid TLA generated-state floor".to_owned())
                    })?;
                let minimum_distinct = configuration(bundle, "minimum_distinct_states")?
                    .parse::<u64>()
                    .map_err(|_| {
                        AggregateError::new("invalid TLA distinct-state floor".to_owned())
                    })?;
                if summary.generated_states >= minimum_generated
                    && summary.distinct_states >= minimum_distinct
                {
                    CheckCompletion::FrontierExhausted
                } else {
                    CheckCompletion::CoverageNotReached
                }
            }
        } else {
            CheckCompletion::HarnessError
        }
    } else {
        CheckCompletion::HarnessError
    };
    let observed = bundle
        .execution
        .checks
        .first()
        .map(|check| check.completion)
        .ok_or_else(|| AggregateError::new("TLA receipt has no check".to_owned()))?;
    if observed != expected {
        return Err(AggregateError::new(format!(
            "TLA receipt completion {observed:?} disagrees with proof artifacts ({expected:?})"
        )));
    }
    Ok(())
}
