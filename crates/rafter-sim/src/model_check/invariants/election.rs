use super::{catalog, summarize, Action, Failure};
use super::{
    check_internal_derived_state, BTreeMap, Cluster, ElectionSafetyExplorer, ExplorationState,
    NodeId, Role, Term,
};

pub(crate) fn check_election_safety(cluster: &Cluster, trace: &[Action]) -> Result<(), Failure> {
    check_internal_derived_state(cluster, trace)?;

    let mut leaders_by_term = BTreeMap::<Term, Vec<NodeId>>::new();
    for (node_id, node) in &cluster.nodes {
        if node.role() == Role::Leader {
            leaders_by_term
                .entry(node.current_term())
                .or_default()
                .push(*node_id);
        }
    }

    for (term, leaders) in leaders_by_term {
        if leaders.len() > 1 {
            return Err(Failure {
                kind: crate::model_check::FailureKind::InvariantViolation,
                invariant: ElectionSafetyExplorer::INVARIANT,
                message: format!("term {term} has multiple leaders: {leaders:?}"),
                trace: trace.to_vec(),
                state: summarize(cluster),
            });
        }
    }

    Ok(())
}

pub(crate) fn check_election_history(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    if let Some(regression) = state.election_history.term_regressions.iter().next() {
        return Err(Failure {
            kind: crate::model_check::FailureKind::InvariantViolation,
            invariant: catalog::EL_01_TERM_MONOTONICITY,
            message: format!(
                "{} term regressed from observed floor {} to {}",
                regression.node_id, regression.previous_floor, regression.observed
            ),
            trace: trace.to_vec(),
            state: summarize(&state.cluster),
        });
    }

    if let Some(conflict) = state.election_history.vote_conflicts.iter().next() {
        return Err(Failure {
            kind: crate::model_check::FailureKind::InvariantViolation,
            invariant: catalog::EL_02_ONE_DURABLE_VOTE_PER_TERM,
            message: format!(
                "{} recorded conflicting durable votes in term {}: {} then {}",
                conflict.node_id, conflict.term, conflict.first_vote, conflict.second_vote
            ),
            trace: trace.to_vec(),
            state: summarize(&state.cluster),
        });
    }

    if let Some(loss) = state.election_history.vote_losses.iter().next() {
        return Err(Failure {
            kind: crate::model_check::FailureKind::InvariantViolation,
            invariant: catalog::EL_02_ONE_DURABLE_VOTE_PER_TERM,
            message: format!(
                "{} lost durable vote for {} in term {}",
                loss.node_id, loss.previous_vote, loss.term
            ),
            trace: trace.to_vec(),
            state: summarize(&state.cluster),
        });
    }

    for grant in &state.election_history.vote_grants {
        if !grant.voter_membership.contains_voter(grant.candidate_id) {
            return Err(Failure {
                kind: crate::model_check::FailureKind::InvariantViolation,
                invariant: catalog::EL_03_SAFE_VOTE_ELIGIBILITY,
                message: format!(
                    "{} granted term {} vote to non-voter {} in membership {:?}",
                    grant.voter_id, grant.term, grant.candidate_id, grant.voter_membership
                ),
                trace: trace.to_vec(),
                state: summarize(&state.cluster),
            });
        }
        if (
            grant.candidate_last_log_term,
            grant.candidate_last_log_index,
        ) < (grant.voter_last_log_term, grant.voter_last_log_index)
        {
            return Err(Failure {
                kind: crate::model_check::FailureKind::InvariantViolation,
                invariant: catalog::EL_03_SAFE_VOTE_ELIGIBILITY,
                message: format!(
                    "{} granted term {} vote to {} with stale candidate log ({}, {}) below voter log ({}, {})",
                    grant.voter_id,
                    grant.term,
                    grant.candidate_id,
                    grant.candidate_last_log_index,
                    grant.candidate_last_log_term,
                    grant.voter_last_log_index,
                    grant.voter_last_log_term,
                ),
                trace: trace.to_vec(),
                state: summarize(&state.cluster),
            });
        }
        if grant.durable_vote != Some(grant.candidate_id) {
            return Err(Failure {
                kind: crate::model_check::FailureKind::InvariantViolation,
                invariant: catalog::EL_02_ONE_DURABLE_VOTE_PER_TERM,
                message: format!(
                    "{} granted term {} vote to {} but durable vote is {:?}",
                    grant.voter_id, grant.term, grant.candidate_id, grant.durable_vote
                ),
                trace: trace.to_vec(),
                state: summarize(&state.cluster),
            });
        }
    }

    if let Some(violation) = state
        .election_history
        .authority_transition_violations
        .first()
    {
        let reason = match violation.reason {
            super::super::state::AuthorityTransitionViolationKind::HigherTermNotFenced => {
                "did not fence higher-term authority"
            }
            super::super::state::AuthorityTransitionViolationKind::StaleTermCreatedLeader => {
                "let stale-term traffic create leadership"
            }
        };
        return Err(Failure {
            kind: crate::model_check::FailureKind::InvariantViolation,
            invariant: catalog::EL_07_TERM_AND_AUTHORITY_FENCING,
            message: format!(
                "{} {reason}: delivered {} term {} from term {} {} to term {} {}",
                violation.node_id,
                violation.message_kind,
                violation.message_term,
                violation.before_term,
                violation.before_role,
                violation.after_term,
                violation.after_role,
            ),
            trace: trace.to_vec(),
            state: summarize(&state.cluster),
        });
    }

    if let Some(conflict) = state.election_history.conflicting_elections.iter().next() {
        return Err(Failure {
            kind: crate::model_check::FailureKind::InvariantViolation,
            invariant: catalog::EL_05_ELECTION_SAFETY_OVER_HISTORY,
            message: format!(
                "term {} elected both {} and {}",
                conflict.term, conflict.first_leader, conflict.second_leader
            ),
            trace: trace.to_vec(),
            state: summarize(&state.cluster),
        });
    }

    for (term, certificate) in &state.election_history.elected_by_term {
        if certificate.term != *term {
            return Err(Failure {
                kind: crate::model_check::FailureKind::InvariantViolation,
                invariant: catalog::EL_05_ELECTION_SAFETY_OVER_HISTORY,
                message: format!(
                    "term {term} stores an election certificate for term {}",
                    certificate.term
                ),
                trace: trace.to_vec(),
                state: summarize(&state.cluster),
            });
        }
        if !certificate.membership.contains_voter(certificate.leader_id) {
            return Err(Failure {
                kind: crate::model_check::FailureKind::InvariantViolation,
                invariant: catalog::EL_06_LEADER_HAS_VALID_ELECTION_QUORUM,
                message: format!(
                    "{} became leader in term {} outside the effective voting membership",
                    certificate.leader_id, certificate.term
                ),
                trace: trace.to_vec(),
                state: summarize(&state.cluster),
            });
        }
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
                state: summarize(&state.cluster),
            });
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
                state: summarize(&state.cluster),
            });
        }
    }

    Ok(())
}
