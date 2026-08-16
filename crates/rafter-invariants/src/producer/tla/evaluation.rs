//! TLA+ completion, counterexample precedence, and observation reduction.

use std::collections::{BTreeMap, BTreeSet};

use crate::evidence::{
    CheckCompletion, ContinuationOutcome, PrimaryCompletionPolicy, PRIMARY_COMPLETION_KEY,
};

use super::{
    checkpoint::RecoveryStatus,
    contract::required_configuration,
    execution::{MainStatus, ObligationFailure, ProbeStatus, TlaExecution},
    tla_output::REQUIRED_MODEL_TRANSITIONS,
};

pub(in crate::producer) enum TlaVerdict {
    Pass,
    /// A reporting continuation that spent its budget with a healthy open
    /// frontier. It passes exactly as `Pass` does -- the obligations carried
    /// the gate -- but it drained nothing, so it records a different
    /// completion. Splitting the verdict rather than the completion mapping
    /// keeps the two pass shapes apart at the point where they are decided.
    PassBudgetElapsed,
    Violation(String),
    Incomplete(CheckCompletion, String),
    Error(String),
}

impl TlaVerdict {
    pub(super) const fn completion(&self) -> CheckCompletion {
        match self {
            Self::Pass => CheckCompletion::FrontierExhausted,
            Self::PassBudgetElapsed => CheckCompletion::BudgetElapsedFrontierOpen,
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
    // Obligations run before the primary configuration, so a failed obligation
    // means the main check never started. It is reported as a harness-level
    // failure rather than a counterexample because obligations carry no
    // registry evidence bindings: they strengthen the layer, and a refutation
    // inside one is a red result that a human must read, not a verdict this
    // layer may silently attach to one predicate.
    //
    // The two failure shapes get two messages. On the scheduled tiers the
    // obligations *are* the gate, so "this theorem did not hold" and "this
    // harness never funded the run" would otherwise arrive at an operator
    // wearing the same words, and only one of them is about the model.
    if execution.obligations.status == ProbeStatus::Failed {
        return match &execution.obligations.failure {
            Some(ObligationFailure::Undischarged(detail)) => {
                error(&format!("TLA proof obligation did not discharge: {detail}"))
            }
            Some(ObligationFailure::Underfunded(detail)) => error(&format!(
                "TLA proof obligation ran out of execution budget: {detail}"
            )),
            None => error("TLA proof obligation failed without a diagnosis"),
        };
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
    let Some(policy) = primary_completion_policy(configuration) else {
        return error("TLA runner configuration omitted a reviewed primary_completion policy");
    };
    if execution.main_status == MainStatus::TimedOut {
        // The frontier is still open. Under a gating policy that is incomplete
        // coverage; under a reporting policy it is the expected shape of a
        // continuation and the obligations have already carried the gate. The
        // progress frame still has to be well formed either way -- a timed-out
        // log that cannot be read is a harness error, not a report.
        if policy.gates() {
            return incomplete(
                CheckCompletion::Timeout,
                "TLC exhausted its soft time budget",
            );
        }
        if execution.main_progress.is_none() {
            return error("reporting TLC continuation omitted a complete progress frame");
        }
        return TlaVerdict::PassBudgetElapsed;
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
    if symbols.is_empty() {
        return error("TLA registry bound no predicates to this check");
    }
    if !policy.gates() {
        // A reporting continuation that did drain is reported, not gated: its
        // counters are published as observations and the floors it did or did
        // not clear are context beside them.
        return TlaVerdict::Pass;
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
    {
        return incomplete(
            CheckCompletion::CoverageNotReached,
            "TLC completion did not satisfy the configured state/depth floor",
        );
    }
    TlaVerdict::Pass
}

pub(in crate::producer) fn primary_completion_policy(
    configuration: &BTreeMap<String, String>,
) -> Option<PrimaryCompletionPolicy> {
    PrimaryCompletionPolicy::parse(configuration.get(PRIMARY_COMPLETION_KEY)?)
}

/// How the primary continuation ended, read only off its own parsed evidence.
///
/// This is the receipt's factual record and is derived independently of the
/// verdict: a reporting profile whose continuation elapsed still says so, and a
/// gating profile that failed its floors still reports the frontier it drained.
pub(in crate::producer) fn continuation_outcome(execution: &TlaExecution) -> ContinuationOutcome {
    if execution
        .main
        .as_ref()
        .is_some_and(|summary| summary.violated_invariant.is_some())
    {
        return ContinuationOutcome::Counterexample;
    }
    if execution.main_status == MainStatus::TimedOut {
        return ContinuationOutcome::BudgetElapsedFrontierOpen;
    }
    match execution.main.as_ref() {
        Some(summary) if summary.states_left == 0 => ContinuationOutcome::FrontierExhausted,
        _ => ContinuationOutcome::BudgetElapsedFrontierOpen,
    }
}

pub(in crate::producer) fn observations(
    execution: &TlaExecution,
    symbols: &BTreeSet<String>,
    configured_invariants: usize,
    policy: PrimaryCompletionPolicy,
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
    }
    // `checked:` records that a predicate was checked over an exhausted state
    // space. Under a gating policy the primary continuation is that space.
    // Under a reporting policy it is the obligations, which bind the same
    // predicates and did drain -- and which the contract requires to be
    // non-empty precisely so this claim always has a source.
    if checked_predicates_are_earned(execution, policy) {
        for symbol in symbols {
            observations.insert(format!("checked:{symbol}"), 1);
        }
    }
    observations
}

fn checked_predicates_are_earned(
    execution: &TlaExecution,
    policy: PrimaryCompletionPolicy,
) -> bool {
    if policy.gates() {
        return execution.main_status == MainStatus::Succeeded
            && execution
                .main
                .as_ref()
                .is_some_and(|summary| summary.completed_without_error);
    }
    execution.obligations.status == ProbeStatus::Passed
}

fn error(message: &str) -> TlaVerdict {
    TlaVerdict::Error(message.to_owned())
}

fn incomplete(completion: CheckCompletion, message: &str) -> TlaVerdict {
    TlaVerdict::Incomplete(completion, message.to_owned())
}
