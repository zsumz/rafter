use super::{summarize, Action, ExplorationState, Failure};

pub(crate) fn check_log_history(state: &ExplorationState, trace: &[Action]) -> Result<(), Failure> {
    if let Some(violation) = state.logical_log_history().violations.iter().next() {
        return Err(Failure {
            kind: crate::model_check::FailureKind::InvariantViolation,
            invariant: violation.invariant,
            message: violation.message.clone(),
            trace: trace.to_vec(),
            state: summarize(state.cluster()),
        });
    }
    if let Some((node_id, transfer_id, index, term)) = state
        .logical_log_history()
        .unwitnessed_snapshots
        .iter()
        .next()
    {
        return Err(Failure {
            kind: crate::model_check::FailureKind::CoverageNotReached,
            invariant: super::catalog::LG_03_LOG_MATCHING,
            message: format!(
                "{node_id} snapshot {transfer_id} at ({index}, term {term}) has no logical-prefix witness"
            ),
            trace: trace.to_vec(),
            state: summarize(state.cluster()),
        });
    }
    Ok(())
}

pub(crate) fn check_commit_history(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    if let Some(violation) = state.commit_history().violations.iter().next() {
        return Err(Failure {
            kind: crate::model_check::FailureKind::InvariantViolation,
            invariant: violation.invariant,
            message: violation.message.clone(),
            trace: trace.to_vec(),
            state: summarize(state.cluster()),
        });
    }
    if let Some((node_id, index)) = state
        .commit_history()
        .unwitnessed_committed_prefixes
        .iter()
        .next()
    {
        return Err(Failure {
            kind: crate::model_check::FailureKind::CoverageNotReached,
            invariant: super::catalog::LG_05_LEADER_COMPLETENESS,
            message: format!(
                "{node_id} committed through {index} without a logical-prefix witness"
            ),
            trace: trace.to_vec(),
            state: summarize(state.cluster()),
        });
    }
    if let Some(index) = state
        .commit_history()
        .unwitnessed_commit_terms
        .iter()
        .next()
    {
        return Err(Failure {
            kind: crate::model_check::FailureKind::HarnessError,
            invariant: super::catalog::LG_05_LEADER_COMPLETENESS,
            message: format!("committed prefix index {index} has no commit-authority term witness"),
            trace: trace.to_vec(),
            state: summarize(state.cluster()),
        });
    }
    Ok(())
}
