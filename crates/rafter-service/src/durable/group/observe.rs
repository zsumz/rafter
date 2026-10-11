//! Watches retain the last genuine observation across worker ownership gaps.
use super::{
    ApplicationEntry, DurableApplication, DurableClientOwner, DurableRaftDriver,
    RaftHardStateStore, RaftLogSegment, RaftSnapshotStore, RaftTransport, SnapshotChunkSource,
};
use rafter_app::metrics::RaftGroupMetrics;

pub(super) fn grouped<G>(m: RaftGroupMetrics<()>, group_id: G) -> RaftGroupMetrics<G> {
    RaftGroupMetrics {
        group_id,
        node_id: m.node_id,
        role: m.role,
        term: m.term,
        leader_hint: m.leader_hint,
        commit_index: m.commit_index,
        applied_index: m.applied_index,
        last_log_index: m.last_log_index,
        snapshot_index: m.snapshot_index,
        membership: m.membership,
        replication: m.replication,
        pending_proposals: m.pending_proposals,
        pending_read_barriers: m.pending_read_barriers,
        pending_query_reads: m.pending_query_reads,
        completed_query_reads: m.completed_query_reads,
        reserved_reads: m.reserved_reads,
        fatal_state: m.fatal_state,
    }
}
impl<G, H, L, S, T, A, Tr, R> DurableRaftDriver<G, H, L, S, T, A, Tr, R>
where
    G: Clone + Eq,
    H: RaftHardStateStore + Send + 'static,
    L: RaftLogSegment + Send + 'static,
    S: RaftSnapshotStore + SnapshotChunkSource + Send + 'static,
    T: ApplicationEntry,
    A: DurableApplication<T>,
    Tr: RaftTransport<G>,
    R: Send + 'static,
{
    pub(super) fn publish_metrics(&self) {
        let Some(metrics) = self.owner.as_ref().and_then(DurableClientOwner::metrics) else {
            return;
        };
        let metrics = grouped(metrics, self.shared.group.clone());
        if metrics != self.shared.metrics.current() {
            let _ = self.shared.metrics.publish(metrics);
        }
    }
}
impl<G, H, L, S, T, A, Tr, R> DurableRaftDriver<G, H, L, S, T, A, Tr, R>
where
    G: Clone + Eq,
    T: ApplicationEntry,
    A: DurableApplication<T>,
    R: Send + 'static,
{
    pub(super) fn publish_failure(&self) {
        self.shared.close();
        let mut metrics = self.shared.metrics.current();
        let fatal = rafter_app::group::GroupFatalState::Poisoned {
            reason: "durable driver failed; discard and recover".into(),
        };
        if metrics.fatal_state != fatal {
            metrics.fatal_state = fatal;
            let _ = self.shared.metrics.publish(metrics);
        }
    }
}
