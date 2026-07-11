use super::{summarize, Action, ExplorationState, Failure};

pub(crate) fn check_log_history(state: &ExplorationState, trace: &[Action]) -> Result<(), Failure> {
    if let Some(violation) = state.logical_log_history.violations.iter().next() {
        return Err(Failure {
            kind: crate::model_check::FailureKind::InvariantViolation,
            invariant: violation.invariant,
            message: violation.message.clone(),
            trace: trace.to_vec(),
            state: summarize(&state.cluster),
        });
    }
    Ok(())
}

pub(crate) fn check_commit_history(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    if let Some(violation) = state.commit_history.violations.iter().next() {
        return Err(Failure {
            kind: crate::model_check::FailureKind::InvariantViolation,
            invariant: violation.invariant,
            message: violation.message.clone(),
            trace: trace.to_vec(),
            state: summarize(&state.cluster),
        });
    }
    Ok(())
}
