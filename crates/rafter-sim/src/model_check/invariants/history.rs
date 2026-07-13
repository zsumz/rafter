use std::collections::BTreeSet;

use super::{
    catalog, summarize, Action, ExplorationState, Failure, LogIndex, MembershipConfig, NodeId,
};

pub(crate) fn check_log_history(state: &ExplorationState, trace: &[Action]) -> Result<(), Failure> {
    check_append_entries_prev_log_acceptance(state, trace)?;
    check_append_entries_stored_suffix_acceptance(state, trace)?;
    if let Some(violation) = state.logical_log_history().violations.iter().next() {
        return Err(history_failure(
            state,
            trace,
            violation.invariant,
            violation.message.clone(),
        ));
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

pub(super) fn check_append_entries_prev_log_acceptance(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    if let Some(violation) = state
        .logical_log_history()
        .append_prev_log_violations
        .iter()
        .next()
    {
        return Err(history_failure(
            state,
            trace,
            catalog::LG_02_TRUTHFUL_APPEND_ENTRIES_ACCEPTANCE,
            violation.message.clone(),
        ));
    }
    Ok(())
}

pub(super) fn check_append_entries_stored_suffix_acceptance(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    if let Some(violation) = state
        .logical_log_history()
        .append_stored_suffix_violations
        .iter()
        .next()
    {
        return Err(history_failure(
            state,
            trace,
            catalog::LG_02_TRUTHFUL_APPEND_ENTRIES_ACCEPTANCE,
            violation.message.clone(),
        ));
    }
    Ok(())
}

pub(crate) fn check_commit_history(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    check_committed_prefix_history_stability(state, trace)?;
    check_stable_commit_quorums(state, trace)?;
    check_joint_commit_quorums(state, trace)?;
    check_current_term_commit_certificates(state, trace)?;
    if let Some(violation) = state.commit_history().violations.iter().next() {
        return Err(history_failure(
            state,
            trace,
            violation.invariant,
            violation.message.clone(),
        ));
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

pub(super) fn check_committed_prefix_history_stability(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    if let Some(violation) = state
        .commit_history()
        .violations
        .iter()
        .find(|violation| violation.invariant == catalog::LG_04_COMMITTED_PREFIX_STABILITY)
    {
        return Err(history_failure(
            state,
            trace,
            catalog::LG_04_COMMITTED_PREFIX_STABILITY,
            violation.message.clone(),
        ));
    }
    Ok(())
}

pub(super) fn check_stable_commit_quorums(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    for certificate in state.commit_history().certificates.values() {
        if !matches!(certificate.membership, MembershipConfig::Stable(_))
            || certificate
                .membership
                .has_quorum(certificate.stored_by.iter().copied())
        {
            continue;
        }
        return Err(invalid_commit_quorum_failure(
            state,
            trace,
            certificate.leader_id,
            certificate.committed_through,
            &certificate.stored_by,
            &certificate.membership,
        ));
    }
    Ok(())
}

pub(super) fn check_joint_commit_quorums(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    for certificate in state.commit_history().certificates.values() {
        if !matches!(certificate.membership, MembershipConfig::Joint(_))
            || certificate
                .membership
                .has_quorum(certificate.stored_by.iter().copied())
        {
            continue;
        }
        return Err(invalid_commit_quorum_failure(
            state,
            trace,
            certificate.leader_id,
            certificate.committed_through,
            &certificate.stored_by,
            &certificate.membership,
        ));
    }
    Ok(())
}

pub(super) fn check_current_term_commit_certificates(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    for certificate in state.commit_history().certificates.values() {
        if certificate.candidate_term == certificate.leader_term {
            continue;
        }
        return Err(history_failure(
            state,
            trace,
            catalog::CM_03_LEADERS_ONLY_COMMIT_CURRENT_TERM_ENTRIES,
            format!(
                "{} advanced commit to {} for term {} while leading term {}",
                certificate.leader_id,
                certificate.committed_through,
                certificate.candidate_term,
                certificate.leader_term
            ),
        ));
    }
    Ok(())
}

fn invalid_commit_quorum_failure(
    state: &ExplorationState,
    trace: &[Action],
    leader_id: NodeId,
    committed_through: LogIndex,
    stored_by: &BTreeSet<NodeId>,
    membership: &MembershipConfig,
) -> Failure {
    history_failure(
        state,
        trace,
        catalog::CM_02_COMMIT_REQUIRES_EFFECTIVE_QUORUM,
        format!(
            "{leader_id} committed {committed_through} without an effective quorum; stored_by={stored_by:?}, membership={membership:?}"
        ),
    )
}

fn history_failure(
    state: &ExplorationState,
    trace: &[Action],
    invariant: &'static str,
    message: String,
) -> Failure {
    Failure {
        kind: crate::model_check::FailureKind::InvariantViolation,
        invariant,
        message,
        trace: trace.to_vec(),
        state: summarize(state.cluster()),
    }
}
