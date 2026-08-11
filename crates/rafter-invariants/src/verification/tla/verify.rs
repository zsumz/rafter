//! Orchestration of authenticated TLA+ model-check evidence acceptance.

use std::path::Path;

use crate::{
    evidence::{
        format::tla::{MEMBERSHIP_TRACE_MIN_DEPTH, MEMBERSHIP_TRACE_MIN_DISTINCT_STATES},
        ResultBundle,
    },
    verification::{AggregateError, AuthenticatedArtifacts},
};

use super::{
    artifact::read_kind,
    checkpoint::verify_checkpoint_authenticated,
    completion::{verify_completion, verify_counterexample_binding},
    detector,
    invocation::{optional_process_log, read_initial_process_log},
    obligation, observation,
    source::{verify_source_binding, verify_tool_pin},
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
    let derived = observation::derive(
        &symbols,
        trace_passed,
        detector_observations,
        obligation_observations,
        checkpoint.as_ref(),
        main_progress,
        main.as_ref(),
        main_summary.as_ref(),
    );
    if check.observations != derived {
        return Err(AggregateError::new(
            "TLA receipt observations disagree with framed proof logs".to_owned(),
        ));
    }
    let violated = main_summary
        .as_ref()
        .and_then(|summary| summary.violated_invariant.as_deref());
    verify_counterexample_binding(bundle, violated)?;
    verify_completion(
        bundle,
        trace_passed,
        detectors_passed,
        obligations_passed,
        checkpoint.as_ref(),
        main.as_ref(),
        main_summary.as_ref(),
    )?;
    Ok(main_parse_diagnostic
        .into_iter()
        .chain(progress_diagnostic)
        .collect())
}

#[cfg(test)]
pub(crate) fn verify(bundle: &ResultBundle, root: &Path) -> Result<Vec<String>, AggregateError> {
    let authenticated = crate::verification::snapshot_available_artifacts(bundle, root)?;
    verify_authenticated(bundle, root, root, &authenticated)
}
