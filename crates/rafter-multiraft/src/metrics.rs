//! Aggregated many-group metrics.

use rafter_app::metrics::RaftGroupMetrics;

/// Metrics snapshot for every group currently open in a host.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MultiRaftMetrics<G> {
    pub groups: Vec<RaftGroupMetrics<G>>,
}
