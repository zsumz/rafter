use std::collections::BTreeMap;

use rafter::{LogIndex, NodeConfig, NodeId};

use super::super::{
    catalog, explorers::CommitSafetyExplorer, helpers::summarize, state::ExplorationState, Bounds,
    Failure, StateSummary, Summary,
};

/// Explores hand-seeded commit-safety states that previously required long,
/// unlikely prefixes before the critical action was reachable.
///
/// # Errors
///
/// Returns [`Failure`] when any seeded state violates commit safety.
pub fn check_raft_seeded_commit_safety(
    configs: Vec<NodeConfig>,
    bounds: Bounds,
) -> Result<Summary, Failure> {
    let seeds = [
        ExplorationState::seeded_low_empty_probe(configs.clone()),
        ExplorationState::seeded_divergent_suffix_probe(configs),
    ];
    let mut explorer = CommitSafetyExplorer::new(bounds);
    for state in seeds {
        let mut trace = Vec::new();
        explorer.explore(&state, &mut trace, 0)?;
    }
    Ok(explorer.summary())
}

/// Explores hand-seeded leadership no-op states.
///
/// These seeds pin the cases where a newly elected leader's no-op can
/// immediately commit prior-term application or configuration entries. The
/// checker fails both on safety violations and on bounds too shallow to reach
/// the seeded commit points.
///
/// # Errors
///
/// Returns [`Failure`] when any seeded state violates election or commit
/// safety, or when the bound does not reach every required seeded observation.
pub fn check_raft_leadership_noop_safety(bounds: Bounds) -> Result<Summary, Failure> {
    let seeds = vec![
        ExplorationState::seeded_single_voter_prior_application_noop(),
        ExplorationState::seeded_single_voter_prior_configuration_noop(),
        ExplorationState::seeded_joint_self_quorum_prior_application_noop(),
        ExplorationState::seeded_leadership_transfer_noop_commit(),
    ];
    let required_applies = required_state_summaries(seeds.iter().flat_map(|state| {
        state
            .required_applied_payloads()
            .keys()
            .copied()
            .map(move |key| (key, state))
    }));
    let required_configurations = required_state_summaries(seeds.iter().flat_map(|state| {
        state
            .required_committed_configurations()
            .keys()
            .copied()
            .map(move |key| (key, state))
    }));
    let required_commits = required_state_summaries(seeds.iter().flat_map(|state| {
        state
            .required_commit_indexes()
            .iter()
            .copied()
            .map(move |key| (key, state))
    }));

    let mut explorer = CommitSafetyExplorer::new(bounds);
    for state in seeds {
        let mut trace = Vec::new();
        explorer.explore(&state, &mut trace, 0)?;
    }

    for (key, summary) in &required_applies {
        if !explorer.observed_required_applies().contains(key) {
            return Err(Failure {
                kind: crate::model_check::FailureKind::CoverageNotReached,
                invariant: catalog::AP_01_ORDERED_EXACTLY_ONCE_COMMITTED_APPLICATION,
                message: format!(
                    "leadership no-op seed did not reach required Apply for {} at {} within depth {}",
                    key.0,
                    key.1,
                    bounds.max_depth()
                ),
                trace: Vec::new(),
                state: summary.clone(),
            });
        }
    }
    for (key, summary) in &required_configurations {
        if !explorer.observed_required_configurations().contains(key) {
            return Err(Failure {
                kind: crate::model_check::FailureKind::CoverageNotReached,
                invariant: catalog::MB_04_MONOTONE_CONFIGURATION_TRANSITION_AND_IDENTITY,
                message: format!(
                    "leadership no-op seed did not reach required committed configuration for {} at {} within depth {}",
                    key.0,
                    key.1,
                    bounds.max_depth()
                ),
                trace: Vec::new(),
                state: summary.clone(),
            });
        }
    }
    for (key, summary) in &required_commits {
        if !explorer.observed_required_commits().contains(key) {
            return Err(Failure {
                kind: crate::model_check::FailureKind::CoverageNotReached,
                invariant: catalog::CM_01_COMMIT_INDEX_MONOTONICITY_AND_BOUNDS,
                message: format!(
                    "leadership no-op seed did not reach required commit for {} at {} within depth {}",
                    key.0,
                    key.1,
                    bounds.max_depth()
                ),
                trace: Vec::new(),
                state: summary.clone(),
            });
        }
    }

    Ok(explorer.summary())
}

fn required_state_summaries<'a>(
    required: impl IntoIterator<Item = ((NodeId, LogIndex), &'a ExplorationState)>,
) -> BTreeMap<(NodeId, LogIndex), StateSummary> {
    required
        .into_iter()
        .map(|(key, state)| (key, summarize(state.cluster())))
        .collect()
}
