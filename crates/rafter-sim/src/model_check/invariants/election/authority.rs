//! Shared shapes for the authority-fencing and pre-vote clause detectors.
//!
//! Each parent detector names one recorded violation kind and owes the same
//! rendering for it, so a failure reports the delivered message and the
//! before/after authority it moved. Which kinds a suite checks is fixed by the
//! parent; nothing here selects them.

use crate::model_check::state::{AuthorityTransitionViolationKind, PreVoteViolationKind};

use super::{
    catalog, check_pre_vote_leader_stability, check_pre_vote_request_authority,
    check_stale_pre_vote_response_authority, summarize, Action, ExplorationState, Failure,
};

pub(super) fn check_authority_transition_kind(
    state: &ExplorationState,
    trace: &[Action],
    expected: AuthorityTransitionViolationKind,
    reason: &str,
) -> Result<(), Failure> {
    if let Some(violation) = state
        .election_history()
        .authority_transition_violations
        .iter()
        .find(|violation| violation.reason == expected)
    {
        return Err(Failure {
            kind: crate::model_check::FailureKind::InvariantViolation,
            invariant: catalog::EL_07_TERM_AND_AUTHORITY_FENCING,
            message: format!(
                "{} {reason}: delivered {} term {} from term {} {} vote {:?} to term {} {} vote {:?}",
                violation.node_id,
                violation.message_kind,
                violation.message_term,
                violation.before_term,
                violation.before_role,
                violation.before_vote,
                violation.after_term,
                violation.after_role,
                violation.after_vote,
            ),
            trace: trace.to_vec(),
            state: summarize(state.cluster()),
        });
    }

    Ok(())
}

pub(super) fn check_pre_vote_history(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    check_pre_vote_request_authority(state, trace)?;
    check_stale_pre_vote_response_authority(state, trace)?;
    check_pre_vote_leader_stability(state, trace)
}

pub(super) fn check_pre_vote_violation_kind(
    state: &ExplorationState,
    trace: &[Action],
    expected: PreVoteViolationKind,
    reason: &str,
) -> Result<(), Failure> {
    if let Some(violation) = state
        .election_history()
        .pre_vote_violations
        .iter()
        .find(|violation| violation.reason == expected)
    {
        return Err(Failure {
            kind: crate::model_check::FailureKind::InvariantViolation,
            invariant: catalog::EL_08_PRE_VOTE_NON_BINDING,
            message: format!(
                "{} {reason}: delivered {} term {} from term {} {} vote {:?} to term {} {} vote {:?}",
                violation.node_id,
                violation.message_kind,
                violation.message_term,
                violation.before_term,
                violation.before_role,
                violation.before_vote,
                violation.after_term,
                violation.after_role,
                violation.after_vote,
            ),
            trace: trace.to_vec(),
            state: summarize(state.cluster()),
        });
    }

    Ok(())
}
