use rafter::NodeConfig;

use crate::Cluster;

use super::super::{
    catalog,
    explorers::CommitSafetyExplorer,
    helpers::{self, summarize},
    observations::Observation,
    state::ExplorationState,
    Bounds, Failure, FailureKind, StateSummary, Summary,
};

use super::{bounded::check_raft_commit_safety, read::check_raft_read_index_safety};

/// Preserves the full production-config exploration and adds a reached
/// production transition that must advance a real commit index.
///
/// # Errors
///
/// Returns [`Failure`] for a protocol violation, harness error, or missing
/// production-config commit witness.
pub fn check_raft_production_config_commit_safety(
    configs: Vec<NodeConfig>,
    bounds: Bounds,
) -> Result<Summary, Failure> {
    let initial_state = summarize(&Cluster::new(configs.clone()));
    let exhaustive = check_raft_commit_safety(configs.clone(), bounds)?;

    let mut witness_state = ExplorationState::new(Cluster::new(configs));
    helpers::elect_node_one_in_state(&mut witness_state);
    let mut witness_explorer = CommitSafetyExplorer::new(Bounds::new(0));
    let mut trace = Vec::new();
    witness_explorer.explore(&witness_state, &mut trace, 0)?;

    require_observation(
        exhaustive.combined(witness_explorer.summary()),
        Observation::ProductionConfigCommitObserved,
        catalog::CM_01_COMMIT_INDEX_MONOTONICITY_AND_BOUNDS,
        initial_state,
    )
}

/// Requires the exhaustive commit exploration to reach a second application
/// proposal blocked by an occupied one-batch replication window.
///
/// # Errors
///
/// Returns [`Failure`] for a protocol violation, harness error, or missing
/// window-one backpressure witness.
pub fn check_raft_window_one_backpressure_safety(
    configs: Vec<NodeConfig>,
    bounds: Bounds,
) -> Result<Summary, Failure> {
    let initial_state = summarize(&Cluster::new(configs.clone()));
    let summary = check_raft_commit_safety(configs, bounds)?;
    require_observation(
        summary,
        Observation::WindowOneBackpressureObserved,
        catalog::LG_01_LEADER_APPEND_ONLY,
        initial_state,
    )
}

/// Requires the exhaustive read exploration to grant an active-lease read in
/// its registration transition without sending a quorum-confirmation round.
///
/// # Errors
///
/// Returns [`Failure`] for a protocol violation, harness error, or missing
/// lease fast-path witness.
pub fn check_raft_lease_fast_path_read_safety(
    configs: Vec<NodeConfig>,
    bounds: Bounds,
) -> Result<Summary, Failure> {
    let initial_state = summarize(&Cluster::new(configs.clone()));
    let summary = check_raft_read_index_safety(configs, bounds)?;
    require_observation(
        summary,
        Observation::LeaseFastPathReadGranted,
        catalog::RD_03_READ_BARRIER_COVERS_COMMITTED_FLOOR,
        initial_state,
    )
}

pub(super) fn require_observation(
    summary: Summary,
    observation: Observation,
    invariant: &'static str,
    state: StateSummary,
) -> Result<Summary, Failure> {
    if summary.observations.contains(observation) {
        return Ok(summary);
    }
    Err(Failure {
        kind: FailureKind::CoverageNotReached,
        invariant,
        message: format!(
            "semantic purpose witness {} was not reached",
            observation.label()
        ),
        trace: Vec::new(),
        state,
    })
}
