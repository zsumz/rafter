//! Election outcomes and the quorum evidence every certificate owes.
//!
//! A recorded certificate must name one leader per term, cite only voters of
//! its own effective membership, and carry an effective quorum of grants, so a
//! pass here means the election was justified by its own evidence. The
//! clause-level entry points stay in the parent.

use rafter::MembershipConfig;

use super::{
    catalog, check_eligible_leader_certificates, check_joint_election_quorums,
    check_stable_election_quorums, summarize, Action, ExplorationState, Failure,
};

pub(super) fn check_election_outcomes(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    if let Some(conflict) = state.election_history().conflicting_elections.iter().next() {
        return Err(Failure {
            kind: crate::model_check::FailureKind::InvariantViolation,
            invariant: catalog::EL_05_ELECTION_SAFETY_OVER_HISTORY,
            message: format!(
                "term {} elected both {} and {}",
                conflict.term, conflict.first_leader, conflict.second_leader
            ),
            trace: trace.to_vec(),
            state: summarize(state.cluster()),
        });
    }

    if let Some((leader_id, term)) = state
        .election_history()
        .uncertified_seeded_leaders
        .iter()
        .next()
    {
        return Err(Failure {
            kind: crate::model_check::FailureKind::CoverageNotReached,
            invariant: catalog::EL_05_ELECTION_SAFETY_OVER_HISTORY,
            message: format!(
                "{leader_id} was already leader in term {term} when exploration history began"
            ),
            trace: trace.to_vec(),
            state: summarize(state.cluster()),
        });
    }

    Ok(())
}

pub(super) fn check_election_certificates(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    check_eligible_leader_certificates(state, trace)?;
    check_election_certificate_voters(state, trace)?;
    check_stable_election_quorums(state, trace)?;
    check_joint_election_quorums(state, trace)
}

pub(in crate::model_check::invariants) fn check_election_certificate_voters(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    for certificate in state.election_history().elected_by_term.values().flatten() {
        if let Some(non_voter) = certificate
            .granted_by
            .iter()
            .find(|voter| !certificate.membership.contains_voter(**voter))
        {
            return Err(Failure {
                kind: crate::model_check::FailureKind::InvariantViolation,
                invariant: catalog::EL_06_LEADER_HAS_VALID_ELECTION_QUORUM,
                message: format!(
                    "{} election certificate for term {} includes non-voter grant {}",
                    certificate.leader_id, certificate.term, non_voter
                ),
                trace: trace.to_vec(),
                state: summarize(state.cluster()),
            });
        }
    }

    Ok(())
}

pub(super) fn check_election_quorums(
    state: &ExplorationState,
    trace: &[Action],
    joint: bool,
) -> Result<(), Failure> {
    for certificate in state.election_history().elected_by_term.values().flatten() {
        let is_joint = matches!(certificate.membership, MembershipConfig::Joint(_));
        if is_joint != joint {
            continue;
        }
        if !certificate
            .membership
            .has_quorum(certificate.granted_by.iter().copied())
        {
            return Err(Failure {
                kind: crate::model_check::FailureKind::InvariantViolation,
                invariant: catalog::EL_06_LEADER_HAS_VALID_ELECTION_QUORUM,
                message: format!(
                    "{} election certificate for term {} lacks an effective quorum; grants={:?}, membership={:?}, last_log=({}, {})",
                    certificate.leader_id,
                    certificate.term,
                    certificate.granted_by,
                    certificate.membership,
                    certificate.last_log_index,
                    certificate.last_log_term,
                ),
                trace: trace.to_vec(),
                state: summarize(state.cluster()),
            });
        }
    }

    Ok(())
}
