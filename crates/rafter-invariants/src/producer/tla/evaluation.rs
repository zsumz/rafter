//! TLA+ completion, counterexample precedence, and observation reduction.

use std::collections::{BTreeMap, BTreeSet};

use crate::evidence::CheckCompletion;

use super::{
    checkpoint::RecoveryStatus,
    contract::required_configuration,
    execution::{MainStatus, ProbeStatus, TlaExecution},
    tla_output::REQUIRED_MODEL_TRANSITIONS,
};

pub(in crate::producer) enum TlaVerdict {
    Pass,
    Violation(String),
    Incomplete(CheckCompletion, String),
    Error(String),
}

impl TlaVerdict {
    pub(super) const fn completion(&self) -> CheckCompletion {
        match self {
            Self::Pass => CheckCompletion::FrontierExhausted,
            Self::Violation(_) => CheckCompletion::Counterexample,
            Self::Incomplete(completion, _) => *completion,
            Self::Error(_) => CheckCompletion::HarnessError,
        }
    }
}

pub(in crate::producer) fn evaluate(
    execution: &TlaExecution,
    symbols: &BTreeSet<String>,
    configuration: &BTreeMap<String, String>,
) -> TlaVerdict {
    if execution.trace_status != ProbeStatus::Passed {
        return error("TLC trace-sample harness did not complete successfully");
    }
    if execution.detector_status != ProbeStatus::Passed {
        return error("TLC negative detector did not report its named counterexample");
    }
    // Obligations run before the primary configuration, so an undischarged
    // obligation means the main check never started. It is reported as a
    // harness-level failure rather than a counterexample because obligations
    // carry no registry evidence bindings: they strengthen the layer, and a
    // refutation inside one is a red result that a human must read, not a
    // verdict this layer may silently attach to one predicate.
    if execution.obligations.status == ProbeStatus::Failed {
        let detail = execution
            .obligations
            .failure
            .as_deref()
            .unwrap_or("proof obligation failed without a diagnosis");
        return error(&format!("TLA proof obligation did not discharge: {detail}"));
    }
    if let Some(invariant) = execution
        .main
        .as_ref()
        .and_then(|summary| summary.violated_invariant.as_ref())
    {
        if symbols.contains(invariant) {
            return TlaVerdict::Violation(invariant.clone());
        }
        return error(&format!(
            "TLC violated unregistered harness predicate {invariant}"
        ));
    }
    if let Some(checkpoint_error) = &execution.checkpoint_error {
        return error(&format!(
            "TLC checkpoint recovery rejected: {checkpoint_error}"
        ));
    }
    if execution.main_status == MainStatus::TimedOut {
        return incomplete(
            CheckCompletion::Timeout,
            "TLC exhausted its soft time budget",
        );
    }
    if let Some(parse_error) = &execution.main_parse_error {
        return error(&format!("malformed TLC tool output: {parse_error}"));
    }
    let Some(summary) = execution.main.as_ref() else {
        return error("TLC model check was not executed");
    };
    if execution.main_status != MainStatus::Succeeded
        || !summary.completed_without_error
        || !summary.process_finished
    {
        return error("TLC exited without a successful completion verdict");
    }
    let minimum_generated = required_configuration(configuration, "minimum_generated_states")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(u64::MAX);
    let minimum_distinct = required_configuration(configuration, "minimum_distinct_states")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(u64::MAX);
    if summary.states_left != 0
        || summary.generated_states < minimum_generated
        || summary.distinct_states < minimum_distinct
        || summary.search_depth == 0
        || symbols.is_empty()
    {
        return incomplete(
            CheckCompletion::CoverageNotReached,
            "TLC completion did not satisfy the configured state/depth floor",
        );
    }
    TlaVerdict::Pass
}

pub(in crate::producer) fn observations(
    execution: &TlaExecution,
    symbols: &BTreeSet<String>,
    configured_invariants: usize,
) -> BTreeMap<String, u64> {
    let mut observations = BTreeMap::from([
        (
            "configured_invariants".to_owned(),
            configured_invariants as u64,
        ),
        ("tool_pin_verified".to_owned(), 1),
        (
            "trace_sample_passed".to_owned(),
            u64::from(execution.trace_status == ProbeStatus::Passed),
        ),
    ]);
    if execution.trace_status == ProbeStatus::Passed {
        observations.extend(
            REQUIRED_MODEL_TRANSITIONS
                .into_iter()
                .map(|transition| (format!("transition_covered:{transition}"), 1)),
        );
    }
    observations.extend(execution.detector_qualifications.clone());
    observations.extend(execution.obligations.observations.clone());
    if let Some(report) = &execution.checkpoint_report {
        observations.extend([
            ("checkpoint_enabled".to_owned(), 1),
            (
                "checkpoint_candidate_present".to_owned(),
                u64::from(report.candidate_present),
            ),
            (
                "checkpoint_compatible".to_owned(),
                u64::from(report.status != RecoveryStatus::Incompatible),
            ),
            (
                "checkpoint_recovery_attempted".to_owned(),
                u64::from(report.recovery_attempted),
            ),
        ]);
    }
    if execution.main_status == MainStatus::TimedOut {
        if let Some(progress) = execution.main_progress {
            observations.extend([
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
    } else if let Some(summary) = &execution.main {
        observations.extend([
            ("generated_states".to_owned(), summary.generated_states),
            ("distinct_states".to_owned(), summary.distinct_states),
            ("states_left_on_queue".to_owned(), summary.states_left),
            ("search_depth".to_owned(), summary.search_depth),
        ]);
        if execution.main_status == MainStatus::Succeeded && summary.completed_without_error {
            for symbol in symbols {
                observations.insert(format!("checked:{symbol}"), 1);
            }
        }
    }
    observations
}

fn error(message: &str) -> TlaVerdict {
    TlaVerdict::Error(message.to_owned())
}

fn incomplete(completion: CheckCompletion, message: &str) -> TlaVerdict {
    TlaVerdict::Incomplete(completion, message.to_owned())
}
