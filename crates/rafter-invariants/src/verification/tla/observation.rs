//! Independent rederivation of TLA+ progress and coverage observations.

use std::collections::BTreeMap;

use crate::{
    evidence::format::{
        process::ProcessLog,
        tla::{parse_latest_progress, TlcProgress, TlcSummary, REQUIRED_MODEL_TRANSITIONS},
    },
    verification::AggregateError,
};

use crate::evidence::format::tla::checkpoint::{RecoveryReport, RecoveryStatus};

pub(super) fn parse_main_summary(
    main: Option<&ProcessLog>,
) -> (Option<TlcSummary>, Option<String>) {
    let Some(log) = main else {
        return (None, None);
    };
    match crate::evidence::format::tla::parse(log.stdout.as_bytes()) {
        Ok(summary) => (Some(summary), None),
        Err(error) => {
            match crate::evidence::format::tla::parse_complete_prefix(log.stdout.as_bytes()) {
                Ok(summary) if summary.violated_invariant.is_some() => (
                    Some(summary),
                    Some(format!("parse TLA main output: {error}")),
                ),
                _ => (None, None),
            }
        }
    }
}

pub(super) fn derive(
    symbols: &[String],
    trace_passed: bool,
    detector_observations: BTreeMap<String, u64>,
    obligation_observations: BTreeMap<String, u64>,
    checkpoint: Option<&RecoveryReport>,
    main_progress: Option<TlcProgress>,
    main: Option<&ProcessLog>,
    main_summary: Option<&TlcSummary>,
) -> BTreeMap<String, u64> {
    let mut derived = BTreeMap::from([
        ("configured_invariants".to_owned(), symbols.len() as u64),
        ("tool_pin_verified".to_owned(), 1),
        ("trace_sample_passed".to_owned(), u64::from(trace_passed)),
    ]);
    if trace_passed {
        derived.extend(
            REQUIRED_MODEL_TRANSITIONS
                .into_iter()
                .map(|transition| (format!("transition_covered:{transition}"), 1)),
        );
    }
    derived.extend(detector_observations);
    derived.extend(obligation_observations);
    if let Some(checkpoint) = checkpoint {
        derived.extend([
            ("checkpoint_enabled".to_owned(), 1),
            (
                "checkpoint_candidate_present".to_owned(),
                u64::from(checkpoint.candidate_present),
            ),
            (
                "checkpoint_compatible".to_owned(),
                u64::from(checkpoint.status != RecoveryStatus::Incompatible),
            ),
            (
                "checkpoint_recovery_attempted".to_owned(),
                u64::from(checkpoint.recovery_attempted),
            ),
        ]);
    }
    if main.is_some_and(|log| log.timed_out) {
        if let Some(progress) = main_progress {
            derived.extend([
                (
                    "progress_generated_states".to_owned(),
                    progress.generated_states,
                ),
                (
                    "progress_distinct_states".to_owned(),
                    progress.distinct_states,
                ),
                ("progress_states_left".to_owned(), progress.states_left),
                ("progress_depth".to_owned(), progress.depth),
            ]);
        }
    } else if let Some(summary) = main_summary {
        derived.extend([
            ("generated_states".to_owned(), summary.generated_states),
            ("distinct_states".to_owned(), summary.distinct_states),
            ("states_left_on_queue".to_owned(), summary.states_left),
            ("search_depth".to_owned(), summary.search_depth),
        ]);
        if main.is_some_and(|log| successful_log(log) && successful_summary(summary)) {
            for symbol in symbols.iter().filter(|symbol| symbol.as_str() != "TypeOK") {
                derived.insert(format!("checked:{symbol}"), 1);
            }
        }
    }
    derived
}

pub(super) fn timeout_progress(
    main: Option<&ProcessLog>,
    main_has_violation: bool,
) -> Result<(Option<TlcProgress>, Option<String>), AggregateError> {
    let Some(log) = main.filter(|log| log.timed_out) else {
        return Ok((None, None));
    };
    let progress = match parse_latest_progress(log.stdout.as_bytes()) {
        Ok(progress) => progress,
        Err(error) if main_has_violation => {
            return Ok((None, Some(format!("parse timed-out TLA progress: {error}"))));
        }
        Err(error) => {
            return Err(AggregateError::new(format!(
                "parse timed-out TLA progress: {error}"
            )));
        }
    };
    if main_has_violation || progress.is_some() {
        return Ok((progress, None));
    }
    Err(AggregateError::new(
        "timed-out TLA log omitted a complete progress frame".to_owned(),
    ))
}

pub(super) fn configured_invariants(source: &str) -> Vec<String> {
    let mut invariants = Vec::new();
    let mut collecting = false;
    for line in source.lines() {
        let line = line.trim();
        if line == "INVARIANT" || line == "INVARIANTS" {
            collecting = true;
        } else if let Some(symbol) = line.strip_prefix("INVARIANT ") {
            invariants.push(symbol.trim().to_owned());
            collecting = false;
        } else if collecting && line.is_empty() {
            collecting = false;
        } else if collecting {
            invariants.push(line.to_owned());
        }
    }
    invariants
}

pub(super) fn successful_log(log: &ProcessLog) -> bool {
    log.exit_code == Some(0) && !log.timed_out
}

pub(super) fn successful_summary(summary: &TlcSummary) -> bool {
    summary.completed_without_error
        && summary.process_finished
        && summary.states_left == 0
        && summary.search_depth > 0
}
