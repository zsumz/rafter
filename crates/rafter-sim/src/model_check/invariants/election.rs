//! The election-safety detectors the invariant registry pins by name.
//!
//! Every clause-level checker the catalog cites is defined here and keeps the
//! order the history suite owes, so a registry row always resolves to a
//! detector in this file. The shared violation shapes and outcome scans live
//! in the private submodules; nothing there chooses what a suite runs.

use super::{catalog, summarize, Action, Failure};
use super::{
    check_internal_derived_state, BTreeMap, Cluster, ElectionSafetyExplorer, ExplorationState,
    NodeId, Role, Term,
};

mod authority;
mod certificates;
mod votes;

use authority::{check_authority_transition_kind, check_pre_vote_history};
use certificates::{check_election_certificates, check_election_outcomes};
use votes::{check_term_and_vote_history, check_vote_grants};

#[cfg(test)]
pub(super) use certificates::check_election_certificate_voters;
#[cfg(test)]
pub(super) use votes::check_vote_grant_durability;

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
    if let Some(error) = state.transition_instrumentation_errors().iter().next() {
        return Err(Failure {
            kind: error.kind,
            invariant: error.invariant,
            message: error.message.clone(),
            trace: trace.to_vec(),
            state: summarize(state.cluster()),
        });
    }
    check_term_and_vote_history(state, trace)?;
    check_vote_grants(state, trace)?;
    check_authority_transitions(state, trace)?;
    check_pre_vote_history(state, trace)?;
    check_election_outcomes(state, trace)?;
    check_election_certificates(state, trace)
}

pub(super) fn check_vote_candidate_eligibility(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    for grant in &state.election_history().vote_grants {
        if !grant.voter_membership.contains_voter(grant.candidate_id) {
            return Err(Failure {
                kind: crate::model_check::FailureKind::InvariantViolation,
                invariant: catalog::EL_03_SAFE_VOTE_ELIGIBILITY,
                message: format!(
                    "{} granted term {} vote to non-voter {} in membership {:?}",
                    grant.voter_id, grant.term, grant.candidate_id, grant.voter_membership
                ),
                trace: trace.to_vec(),
                state: summarize(state.cluster()),
            });
        }
    }
    Ok(())
}

pub(super) fn check_vote_candidate_log_freshness(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    for grant in &state.election_history().vote_grants {
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
                state: summarize(state.cluster()),
            });
        }
    }
    Ok(())
}

fn check_authority_transitions(state: &ExplorationState, trace: &[Action]) -> Result<(), Failure> {
    check_higher_term_authority_fencing(state, trace)?;
    check_stale_authority_leadership(state, trace)?;
    check_stale_authority_state(state, trace)
}

pub(super) fn check_higher_term_authority_fencing(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    check_authority_transition_kind(
        state,
        trace,
        super::super::state::AuthorityTransitionViolationKind::HigherTermNotFenced,
        "did not fence higher-term authority",
    )
}

pub(super) fn check_stale_authority_leadership(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    check_authority_transition_kind(
        state,
        trace,
        super::super::state::AuthorityTransitionViolationKind::StaleTermCreatedLeader,
        "let stale-term traffic create leadership",
    )
}

pub(super) fn check_stale_authority_state(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    check_authority_transition_kind(
        state,
        trace,
        super::super::state::AuthorityTransitionViolationKind::StaleTermLoweredAuthority,
        "let stale-term traffic lower durable authority",
    )
}

pub(super) fn check_pre_vote_request_authority(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    authority::check_pre_vote_violation_kind(
        state,
        trace,
        super::super::state::PreVoteViolationKind::RequestMutatedAuthority,
        "pre-vote request mutated authority",
    )
}

pub(super) fn check_stale_pre_vote_response_authority(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    authority::check_pre_vote_violation_kind(
        state,
        trace,
        super::super::state::PreVoteViolationKind::StaleResponseAdvancedAuthority,
        "stale pre-vote response advanced authority",
    )
}

pub(super) fn check_pre_vote_leader_stability(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    authority::check_pre_vote_violation_kind(
        state,
        trace,
        super::super::state::PreVoteViolationKind::RequestDisruptedLeader,
        "pre-vote request disrupted a leader",
    )
}

pub(super) fn check_eligible_leader_certificates(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    for (term, certificates) in &state.election_history().elected_by_term {
        for certificate in certificates {
            if certificate.term != *term {
                return Err(Failure {
                    kind: crate::model_check::FailureKind::InvariantViolation,
                    invariant: catalog::EL_05_ELECTION_SAFETY_OVER_HISTORY,
                    message: format!(
                        "term {term} stores an election certificate for term {}",
                        certificate.term
                    ),
                    trace: trace.to_vec(),
                    state: summarize(state.cluster()),
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
                    state: summarize(state.cluster()),
                });
            }
        }
    }

    Ok(())
}

pub(super) fn check_stable_election_quorums(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    certificates::check_election_quorums(state, trace, false)
}

pub(super) fn check_joint_election_quorums(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    certificates::check_election_quorums(state, trace, true)
}
