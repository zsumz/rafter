//! Orchestration of authenticated TLA+ model-check evidence acceptance.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use crate::{
    evidence::{
        format::tla::{MEMBERSHIP_TRACE_MIN_DEPTH, MEMBERSHIP_TRACE_MIN_DISTINCT_STATES},
        PrimaryCompletionPolicy, ResultBundle, PRIMARY_COMPLETION_KEY,
    },
    verification::{AggregateError, AuthenticatedArtifacts},
};

use super::{
    artifact::read_kind,
    checkpoint::verify_checkpoint_authenticated,
    completion::{
        verify_completion, verify_continuation_binding, verify_counterexample_binding,
        CompletionEvidence,
    },
    detector,
    invocation::{optional_process_log, read_initial_process_log},
    obligation, observation,
    source::{configuration, verify_source_binding, verify_tool_pin},
};

pub(crate) fn verify_authenticated(
    bundle: &ResultBundle,
    root: &Path,
    source_root: &Path,
    authenticated: &AuthenticatedArtifacts,
) -> Result<Vec<String>, AggregateError> {
    let check = bundle
        .execution
        .checks
        .first()
        .ok_or_else(|| AggregateError::new("TLA receipt has no check".to_owned()))?;
    let Qualification {
        producer_repository,
        config,
        trace_passed,
        detector_observations,
        detectors_passed,
    } = verify_qualification(bundle, check, root, source_root, authenticated)?;
    let (obligation_observations, obligations_passed) =
        obligation::verify(bundle, check, root, &producer_repository, authenticated)?;
    // Obligations gate the primary run, so a main log after an undischarged
    // obligation is evidence the producer ignored its own ordering.
    if !obligations_passed && super::artifact::has_kind(check, "tla-log")? {
        return Err(AggregateError::new(
            "TLA main log exists after an undischarged proof obligation".to_owned(),
        ));
    }
    let main = optional_process_log(
        bundle,
        check,
        "tla-log",
        "model-check",
        root,
        &producer_repository,
        authenticated,
    )?;
    let (main_summary, main_parse_diagnostic) = observation::parse_main_summary(main.as_ref());
    let main_has_violation = main_summary
        .as_ref()
        .is_some_and(|summary| summary.violated_invariant.is_some());
    let checkpoint =
        verify_checkpoint_authenticated(bundle, check, main_has_violation, authenticated)?;
    let (main_progress, progress_diagnostic) =
        observation::timeout_progress(main.as_ref(), main_has_violation)?;
    let symbols = observation::configured_invariants(&config);
    // The policy is read from the pinned profile contract, never from the
    // receipt: which continuations gate is a contract decision, and a producer
    // does not get to choose it for itself.
    let policy = PrimaryCompletionPolicy::parse(configuration(bundle, PRIMARY_COMPLETION_KEY)?)
        .ok_or_else(|| {
            AggregateError::new("TLA profile pins no reviewed primary_completion policy".to_owned())
        })?;
    let checked_earned = observation::checked_predicates_are_earned(
        policy,
        obligations_passed,
        main.as_ref(),
        main_summary.as_ref(),
    );
    let derived = observation::derive(observation::DerivedObservations {
        symbols: &symbols,
        trace_passed,
        detector_observations,
        obligation_observations,
        checked_predicates_are_earned: checked_earned,
        checkpoint: checkpoint.as_ref(),
        main_progress,
        main: main.as_ref(),
        main_summary: main_summary.as_ref(),
    });
    if check.observations != derived {
        return Err(AggregateError::new(
            "TLA receipt observations disagree with framed proof logs".to_owned(),
        ));
    }
    let violated = main_summary
        .as_ref()
        .and_then(|summary| summary.violated_invariant.as_deref());
    verify_counterexample_binding(bundle, violated)?;
    verify_continuation_binding(bundle, policy, main.as_ref(), main_summary.as_ref())?;
    verify_completion(
        bundle,
        &CompletionEvidence {
            trace_passed,
            detectors_passed,
            obligations_passed,
            policy,
            checkpoint: checkpoint.as_ref(),
            main: main.as_ref(),
            summary: main_summary.as_ref(),
        },
    )?;
    Ok(main_parse_diagnostic
        .into_iter()
        .chain(progress_diagnostic)
        .collect())
}

/// Everything that must hold before the primary continuation's own evidence is
/// worth reading: the source and tool bindings, the membership trace, and the
/// negative detectors.
struct Qualification {
    producer_repository: PathBuf,
    config: String,
    trace_passed: bool,
    detector_observations: BTreeMap<String, u64>,
    detectors_passed: bool,
}

fn verify_qualification(
    bundle: &ResultBundle,
    check: &crate::evidence::CheckReceipt,
    root: &Path,
    source_root: &Path,
    authenticated: &AuthenticatedArtifacts,
) -> Result<Qualification, AggregateError> {
    let (trace, producer_repository) = read_initial_process_log(
        bundle,
        check,
        "tla-trace-log",
        "trace-sample",
        root,
        authenticated,
    )?;
    let config = read_kind(check, "tla-config", authenticated)?;
    let detector_template = read_kind(check, "tla-detector-config", authenticated)?;
    verify_source_binding(bundle, check, source_root, authenticated)?;
    verify_tool_pin(bundle, check, authenticated)?;
    let trace_summary = crate::evidence::format::tla::parse(trace.stdout.as_bytes()).ok();
    let trace_passed = trace_summary.as_ref().is_some_and(|summary| {
        observation::successful_log(&trace)
            && observation::successful_summary(summary)
            && summary.distinct_states >= MEMBERSHIP_TRACE_MIN_DISTINCT_STATES
            && summary.search_depth >= MEMBERSHIP_TRACE_MIN_DEPTH
    });
    let (detector_observations, detectors_passed) = detector::verify(
        bundle,
        check,
        root,
        &producer_repository,
        &detector_template,
        authenticated,
    )?;
    if !trace_passed && super::artifact::has_kind(check, "tla-log")? {
        return Err(AggregateError::new(
            "TLA main log exists after a failed trace probe".to_owned(),
        ));
    }
    Ok(Qualification {
        producer_repository,
        config,
        trace_passed,
        detector_observations,
        detectors_passed,
    })
}

#[cfg(test)]
pub(crate) fn verify(bundle: &ResultBundle, root: &Path) -> Result<Vec<String>, AggregateError> {
    let authenticated = crate::verification::snapshot_available_artifacts(bundle, root)?;
    verify_authenticated(bundle, root, root, &authenticated)
}
