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

/// Everything the expected completion is a function of.
///
/// These arrive from four independent verification passes -- qualification,
/// obligations, checkpoint recovery, and the primary log -- so they are grouped
/// rather than threaded individually through the call chain.
pub(super) struct CompletionEvidence<'a> {
    pub(super) trace_passed: bool,
    pub(super) detectors_passed: bool,
    pub(super) obligations_passed: bool,
    pub(super) policy: PrimaryCompletionPolicy,
    pub(super) checkpoint: Option<&'a RecoveryReport>,
    pub(super) main: Option<&'a ProcessLog>,
    pub(super) summary: Option<&'a TlcSummary>,
}

impl CompletionEvidence<'_> {
    /// An undischarged obligation is a harness-level failure of the layer, in
    /// the same class as a broken detector: the primary run was never entitled
    /// to start, so no completion it might claim is admissible.
    fn qualification_failed(&self) -> bool {
        !self.trace_passed
            || !self.detectors_passed
            || !self.obligations_passed
            || self
                .checkpoint
                .is_some_and(|report| report.status == RecoveryStatus::Incompatible)
    }

    fn violated(&self) -> Option<&str> {
        self.summary
            .and_then(|summary| summary.violated_invariant.as_deref())
    }
}

pub(super) fn verify_completion(
    bundle: &ResultBundle,
    evidence: &CompletionEvidence<'_>,
) -> Result<(), AggregateError> {
    let expected = expected_completion(bundle, evidence)?;
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

fn expected_completion(
    bundle: &ResultBundle,
    evidence: &CompletionEvidence<'_>,
) -> Result<CheckCompletion, AggregateError> {
    if evidence.qualification_failed() {
        return Ok(CheckCompletion::HarnessError);
    }
    if let Some(violated) = evidence.violated() {
        return Ok(if violated == "TypeOK" {
            CheckCompletion::HarnessError
        } else {
            CheckCompletion::Counterexample
        });
    }
    if evidence.main.is_some_and(|log| log.timed_out) {
        // A reporting continuation is expected to end here. It is only a pass
        // when the progress frame is readable, which `timeout_progress` has
        // already required of the same log.
        return Ok(if evidence.policy.gates() {
            CheckCompletion::Timeout
        } else {
            CheckCompletion::FrontierExhausted
        });
    }
    let (Some(main), Some(summary)) = (evidence.main, evidence.summary) else {
        return Ok(CheckCompletion::HarnessError);
    };
    if !successful_log(main) || !successful_summary(summary) {
        return Ok(CheckCompletion::HarnessError);
    }
    if !evidence.policy.gates() {
        return Ok(CheckCompletion::FrontierExhausted);
    }
    gated_floor_completion(bundle, summary)
}

/// Under a gating policy the drained frontier still has to clear the profile's
/// calibrated floors before it counts as exhausted.
fn gated_floor_completion(
    bundle: &ResultBundle,
    summary: &TlcSummary,
) -> Result<CheckCompletion, AggregateError> {
    let floor = |name: &str, label: &str| -> Result<u64, AggregateError> {
        configuration(bundle, name)?
            .parse::<u64>()
            .map_err(|_| AggregateError::new(format!("invalid TLA {label}-state floor")))
    };
    let minimum_generated = floor("minimum_generated_states", "generated")?;
    let minimum_distinct = floor("minimum_distinct_states", "distinct")?;
    Ok(
        if summary.generated_states >= minimum_generated
            && summary.distinct_states >= minimum_distinct
        {
            CheckCompletion::FrontierExhausted
        } else {
            CheckCompletion::CoverageNotReached
        },
    )
}
