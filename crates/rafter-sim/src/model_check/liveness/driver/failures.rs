//! Classified soak failures raised by the liveness monitors.
//!
//! Each constructor fixes the failure kind at the call site — harness error,
//! coverage gap, or invariant violation — so a verification artifact never has
//! to infer severity from the message text.

use crate::model_check::{
    helpers::summarize,
    invariants::run_replay_check,
    soak::{SoakAction, SoakConfig, SoakFailure},
    state::ExplorationState,
    Failure, ReplayCheck,
};

pub(in crate::model_check::liveness) fn check_soak_safety(
    state: &ExplorationState,
    config: SoakConfig,
    trace: &[SoakAction],
) -> Result<(), SoakFailure> {
    if let Err(failure) = run_replay_check(state, ReplayCheck::CommitSafety, &[]) {
        return Err(SoakFailure {
            seed: config.seed,
            step: trace.len(),
            trace: trace.to_vec(),
            failure: Box::new(failure),
        });
    }
    Ok(())
}

pub(in crate::model_check::liveness) fn soak_liveness_coverage_failure(
    state: &ExplorationState,
    config: SoakConfig,
    trace: &[SoakAction],
    invariant: &'static str,
    message: String,
) -> SoakFailure {
    classified_liveness_failure(
        state,
        config,
        trace,
        invariant,
        crate::model_check::FailureKind::CoverageNotReached,
        message,
    )
}

pub(in crate::model_check::liveness) fn soak_liveness_invariant_failure(
    state: &ExplorationState,
    config: SoakConfig,
    trace: &[SoakAction],
    invariant: &'static str,
    message: String,
) -> SoakFailure {
    classified_liveness_failure(
        state,
        config,
        trace,
        invariant,
        crate::model_check::FailureKind::InvariantViolation,
        message,
    )
}

fn classified_liveness_failure(
    state: &ExplorationState,
    config: SoakConfig,
    trace: &[SoakAction],
    invariant: &'static str,
    kind: crate::model_check::FailureKind,
    message: String,
) -> SoakFailure {
    SoakFailure {
        seed: config.seed,
        step: trace.len(),
        trace: trace.to_vec(),
        failure: Box::new(Failure {
            kind,
            invariant,
            message,
            trace: Vec::new(),
            state: summarize(state.cluster()),
        }),
    }
}

pub(in crate::model_check::liveness) fn soak_transition_failure(
    config: SoakConfig,
    trace: &[SoakAction],
    failure: Failure,
) -> SoakFailure {
    SoakFailure {
        seed: config.seed,
        step: trace.len(),
        trace: trace.to_vec(),
        failure: Box::new(failure),
    }
}

pub(in crate::model_check::liveness) fn soak_liveness_harness_error(
    state: &ExplorationState,
    config: SoakConfig,
    trace: &[SoakAction],
    message: &'static str,
) -> SoakFailure {
    SoakFailure {
        seed: config.seed,
        step: trace.len(),
        trace: trace.to_vec(),
        failure: Box::new(Failure {
            kind: crate::model_check::FailureKind::HarnessError,
            invariant: "bounded-fair liveness scheduler",
            message: message.to_owned(),
            trace: Vec::new(),
            state: summarize(state.cluster()),
        }),
    }
}
