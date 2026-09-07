//! Point-in-time views a driver reads without stepping anything.
//!
//! A metrics snapshot and the two committed-application indexes a readiness
//! gate compares its state machine against. Observation only: none of it
//! advances the protocol or the application.

use super::{
    Debug, LogIndex, PersistedRaftRuntime, RaftGroup, RaftGroupMetrics, ReplicatedStateMachine,
};

impl<G, A, R> RaftGroup<G, A, R>
where
    G: Clone + Ord + Debug + Send + Sync + 'static,
    A: ReplicatedStateMachine,
    A::CommandResult: Clone,
    R: PersistedRaftRuntime,
{
    /// Returns a point-in-time metrics snapshot for the group.
    #[must_use]
    pub fn metrics(&self) -> RaftGroupMetrics<G> {
        let applied_index = self.app.applied_index().unwrap_or(self.last_applied_index);
        let pending_read_barriers = self.pending_reads.len();
        RaftGroupMetrics {
            group_id: self.group_id.clone(),
            node_id: self.node_id,
            role: self.raft.role(),
            term: self.raft.current_term(),
            leader_hint: self.raft.leader_hint(),
            commit_index: self.raft.commit_index(),
            applied_index,
            last_log_index: self.raft.last_log_index(),
            snapshot_index: self.raft.snapshot_index(),
            membership: self.raft.membership(),
            replication: self.raft.replication(),
            pending_proposals: self.pending_proposals.len(),
            pending_read_barriers,
            pending_query_reads: self.pending_query_reads.len(),
            completed_query_reads: self.completed_query_reads.len(),
            reserved_reads: self.reserved_read_count(),
            fatal_state: self.fatal_state.clone(),
        }
    }

    /// Returns the index this group's state machine must reach to have applied
    /// every committed application command.
    ///
    /// Compare it with the state machine's own applied index to gate readiness
    /// after recovery:
    ///
    /// ```text
    /// state_machine.applied_index()? >= group.committed_application_index()
    /// ```
    ///
    /// Use `>=`, never equality. A state machine that installed a snapshot whose
    /// boundary sits above the last committed application entry legitimately
    /// reports a higher applied index, as does one seeded through
    /// [`RaftGroup::with_applied_index`].
    ///
    /// The predicate is false while a restarted node still holds recovery outputs
    /// the caller has not applied, which is exactly when a readiness gate must hold
    /// a replica back. It is not a linearizability signal: it proves only that this
    /// replica has applied everything *it* knows to be committed. Group poison does
    /// not change it — a poisoned group reports the same runtime value and will
    /// never apply again, so a readiness gate must check
    /// [`RaftGroup::fatal_state`] as well.
    ///
    /// A caller that compacts at a boundary above the index its state machine
    /// reports applied raises this value past what that state machine will ever
    /// reach. Compact at the applied index, as
    /// [`crate::state_machine::ReplicatedStateMachine::build_snapshot`] already
    /// requires.
    ///
    /// The same per-replica predicate is a cluster harness's convergence wait:
    /// every replica the harness can still reach must satisfy it before the
    /// cluster has settled. Replicas the harness has partitioned or removed
    /// cannot advance and are the caller's to skip.
    #[must_use]
    pub fn committed_application_index(&self) -> LogIndex {
        self.raft.committed_application_index()
    }

    /// Returns the index this group's state machine must reach to have applied
    /// every committed application command at or below `index`.
    ///
    /// Use this to convert a raw log index into a reachable applied floor. A
    /// commit index, a read index, or a snapshot boundary may name an entry the
    /// state machine will never be told about, and waiting for the state machine
    /// to report that index waits forever. An index taken from
    /// [`crate::proposal::ProposalEvent::Applied`] never needs the conversion: it
    /// already names an application entry.
    ///
    /// This is what [`RaftGroup::read`] applies to a granted read index on the
    /// caller's behalf. It is not applied to a caller-supplied
    /// `min_applied_index`; see [`crate::read::ReadRequest`].
    #[must_use]
    pub fn committed_application_index_through(&self, index: LogIndex) -> LogIndex {
        self.raft.committed_application_index_through(index)
    }
}
