use rafter::NodeConfig;

use crate::Cluster;

use super::super::{
    explorers::{CommitSafetyExplorer, ElectionSafetyExplorer},
    helpers,
    state::ExplorationState,
    Bounds, Failure, Summary,
};

/// Exhaustively explores bounded Raft tick and ready-message delivery schedules.
///
/// # Errors
///
/// Returns [`Failure`] when any explored state violates election safety.
pub fn check_raft_election_safety(
    configs: Vec<NodeConfig>,
    bounds: Bounds,
) -> Result<Summary, Failure> {
    let state = ExplorationState::new(Cluster::new(configs));
    let mut explorer = ElectionSafetyExplorer::new(bounds);
    let mut trace = Vec::new();
    explorer.explore(&state, &mut trace, 0)?;
    Ok(explorer.summary())
}

/// Exhaustively explores bounded Raft proposal, tick, and delivery schedules.
///
/// # Errors
///
/// Returns [`Failure`] when any explored state violates commit safety.
pub fn check_raft_commit_safety(
    configs: Vec<NodeConfig>,
    bounds: Bounds,
) -> Result<Summary, Failure> {
    let state = ExplorationState::new(Cluster::new(configs));
    let mut explorer = CommitSafetyExplorer::new(bounds);
    let mut trace = Vec::new();
    explorer.explore(&state, &mut trace, 0)?;
    Ok(explorer.summary())
}

/// Exhaustively explores bounded dynamic-membership proposals mixed with
/// ticks, message delivery, and client proposals.
///
/// # Errors
///
/// Returns [`Failure`] when any explored state violates commit safety or a
/// membership-specific invariant.
pub fn check_raft_membership_safety(
    configs: Vec<NodeConfig>,
    bounds: Bounds,
) -> Result<Summary, Failure> {
    let mut state = ExplorationState::new(Cluster::new(configs));
    helpers::elect_node_one_in_state(&mut state);
    let mut explorer = CommitSafetyExplorer::new(bounds);
    let mut trace = Vec::new();
    explorer.explore(&state, &mut trace, 0)?;
    Ok(explorer.summary())
}
