use rafter::{NodeConfig, NodeId};

use crate::Cluster;

use super::super::{
    explorers::ReadIndexSafetyExplorer, helpers, state::ExplorationState, Bounds, Failure, Summary,
};

/// Exhaustively explores bounded schedules that mix proposals, message
/// faults, and read barriers, checking that every granted barrier covers the
/// cluster-wide committed floor at its registration (thesis 6.4).
///
/// The initial state is an elected leader with one committed current-term
/// entry, so barriers are grantable within shallow depths.
///
/// # Errors
///
/// Returns [`Failure`] when any explored state violates election safety,
/// commit safety, or the read-barrier committed-floor invariant.
pub fn check_raft_read_index_safety(
    configs: Vec<NodeConfig>,
    bounds: Bounds,
) -> Result<Summary, Failure> {
    let mut cluster = Cluster::new(configs);
    helpers::elect_node_one(&mut cluster);
    cluster.propose(NodeId(1), b"read-index-seed".to_vec());
    cluster.deliver_all();
    let state = ExplorationState::new(cluster);
    let mut explorer = ReadIndexSafetyExplorer::new(bounds);
    let mut trace = Vec::new();
    explorer.explore(&state, &mut trace, 0)?;
    Ok(explorer.summary())
}
