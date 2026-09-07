//! Term, vote, and grant history owed by the election suite.
//!
//! These predicates read only the recorded election history, so a passing
//! check means the observed transitions never regressed a term or split a
//! durable vote. The clause-level grant detectors stay in the parent; nothing
//! here decides which suite runs them.

use super::{
    catalog, check_vote_candidate_eligibility, check_vote_candidate_log_freshness, summarize,
    Action, ExplorationState, Failure,
};

pub(super) fn check_term_and_vote_history(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    if let Some(regression) = state.election_history().term_regressions.iter().next() {
        return Err(Failure {
            kind: crate::model_check::FailureKind::InvariantViolation,
            invariant: catalog::EL_01_TERM_MONOTONICITY,
            message: format!(
                "{} term regressed from observed floor {} to {}",
                regression.node_id, regression.previous_floor, regression.observed
            ),
            trace: trace.to_vec(),
            state: summarize(state.cluster()),
        });
    }

    if let Some(conflict) = state.election_history().vote_conflicts.iter().next() {
        return Err(Failure {
            kind: crate::model_check::FailureKind::InvariantViolation,
            invariant: catalog::EL_02_ONE_DURABLE_VOTE_PER_TERM,
            message: format!(
                "{} recorded conflicting durable votes in term {}: {} then {}",
                conflict.node_id, conflict.term, conflict.first_vote, conflict.second_vote
            ),
            trace: trace.to_vec(),
            state: summarize(state.cluster()),
        });
    }

    if let Some(loss) = state.election_history().vote_losses.iter().next() {
        return Err(Failure {
            kind: crate::model_check::FailureKind::InvariantViolation,
            invariant: catalog::EL_02_ONE_DURABLE_VOTE_PER_TERM,
            message: format!(
                "{} lost durable vote for {} in term {}",
                loss.node_id, loss.previous_vote, loss.term
            ),
            trace: trace.to_vec(),
            state: summarize(state.cluster()),
        });
    }

    Ok(())
}

pub(super) fn check_vote_grants(state: &ExplorationState, trace: &[Action]) -> Result<(), Failure> {
    check_vote_candidate_eligibility(state, trace)?;
    check_vote_candidate_log_freshness(state, trace)?;
    check_vote_grant_durability(state, trace)
}

pub(in crate::model_check::invariants) fn check_vote_grant_durability(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    for grant in &state.election_history().vote_grants {
        if grant.durable_vote != Some(grant.candidate_id) {
            return Err(Failure {
                kind: crate::model_check::FailureKind::InvariantViolation,
                invariant: catalog::EL_02_ONE_DURABLE_VOTE_PER_TERM,
                message: format!(
                    "{} granted term {} vote to {} but durable vote is {:?}",
                    grant.voter_id, grant.term, grant.candidate_id, grant.durable_vote
                ),
                trace: trace.to_vec(),
                state: summarize(state.cluster()),
            });
        }
    }

    Ok(())
}
