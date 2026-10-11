//! Read-only metrics for the same incarnation that owns the client queues.
use super::{
    ApplicationEntry, DurableApplication, DurableClientOwner, RaftHardStateStore, RaftLogSegment,
    RaftSnapshotStore, ServiceState, SnapshotChunkSource,
};
use crate::durable::lock;
use rafter_app::{group::GroupFatalState, metrics::RaftGroupMetrics};

impl<H, L, S, T, A, R: Send + 'static> DurableClientOwner<H, L, S, T, A, R>
where
    H: RaftHardStateStore + Send + 'static,
    L: RaftLogSegment + Send + 'static,
    S: RaftSnapshotStore + SnapshotChunkSource + Send + 'static,
    T: ApplicationEntry,
    A: DurableApplication<T>,
{
    /// Observes protocol state, actual application durability, and client phases.
    ///
    /// Returns `None` while persistence owns the node, the application is handed
    /// off for snapshot installation, or the backend has been released. Reading
    /// metrics drives no work and establishes no quorum or readiness proof.
    /// Protocol fields describe local knowledge; check `fatal_state` before
    /// using them. `applied_index` is the verified application durable floor,
    /// never the core's dispatch cursor or the application's submitted position.
    ///
    /// Pending proposals include queued writes. Pending query reads count all
    /// unfinished linearizable queries; barriers count submitted rounds awaiting
    /// their terminal protocol output. Completed query reads count successful proofs
    /// until collection or abandonment. Reserved reads sum unfinished queries
    /// and uncollected proofs. Local reads and completed errors are excluded
    /// from those four read counters; sender `retained()` includes their credits.
    /// The group is `()`, matching this owner's read proofs and routing contract.
    #[must_use]
    pub fn metrics(&self) -> Option<RaftGroupMetrics<()>> {
        let node = self.ready_node()?;
        let applied_index = self.durable_through()?;
        let reads = self
            .reads
            .as_ref()
            .map(|r| r.shared.counts())
            .unwrap_or_default();
        Some(RaftGroupMetrics {
            group_id: (),
            node_id: node.id(),
            role: node.role(),
            term: node.current_term(),
            leader_hint: node.leader_hint(),
            commit_index: node.commit_index(),
            applied_index,
            last_log_index: node.last_log_index(),
            snapshot_index: node.snapshot_index(),
            membership: node.effective_membership(),
            replication: node.leader_replication_progress(),
            pending_proposals: lock(&self.shared.state).entries.len(),
            pending_read_barriers: reads.barriers,
            pending_query_reads: reads.queries,
            completed_query_reads: reads.completed,
            reserved_reads: reads.reserved,
            fatal_state: if self.state() == ServiceState::Failed {
                GroupFatalState::Poisoned {
                    reason: "durable client owner failed; recover from storage".into(),
                }
            } else {
                GroupFatalState::Healthy
            },
        })
    }
}
