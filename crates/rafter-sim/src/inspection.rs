use rafter::{
    BootstrapLogEntry, BootstrapState, CommittedConfiguration, LogEntry, LogIndex,
    MembershipConfig, Node, NodeId, PromotionBarrier, ReplicationProgress, Role, Term,
};

use crate::{
    Applied, Cluster, DurableSnapshotDigest, DurableStateDigest, ReadGranted, ReadRegistered,
    SimClock, SnapshotInstalled,
};

impl Cluster {
    /// Returns the simulator clock.
    #[must_use]
    pub fn clock(&self) -> &SimClock {
        &self.clock
    }

    /// Returns the nodes currently in the leader role.
    #[must_use]
    pub fn leaders(&self) -> Vec<NodeId> {
        self.nodes
            .iter()
            .filter_map(|(node_id, node)| (node.role() == Role::Leader).then_some(*node_id))
            .collect()
    }

    /// Returns the nodes currently leading in `term`.
    #[must_use]
    pub fn leaders_in_term(&self, term: Term) -> Vec<NodeId> {
        self.nodes
            .iter()
            .filter_map(|(node_id, node)| {
                (node.role() == Role::Leader && node.current_term() == term).then_some(*node_id)
            })
            .collect()
    }

    /// Returns the snapshot installations observed by the simulator.
    #[must_use]
    pub fn snapshot_installs(&self) -> &[SnapshotInstalled] {
        &self.snapshot_installs
    }

    /// Returns the read barriers granted by simulated nodes.
    #[must_use]
    pub fn read_grants(&self) -> &[ReadGranted] {
        &self.read_grants
    }

    /// Returns the read barriers registered by simulated clients.
    #[must_use]
    pub fn read_registrations(&self) -> &[ReadRegistered] {
        &self.read_registrations
    }

    /// Returns the highest commit index reported by any simulated node.
    #[must_use]
    pub fn committed_floor(&self) -> LogIndex {
        self.nodes
            .values()
            .map(Node::commit_index)
            .max()
            .unwrap_or_default()
    }

    /// Returns `node_id`'s local applied index.
    #[must_use]
    pub fn local_applied_index(&self, node_id: NodeId) -> LogIndex {
        self.node(node_id).applied_index()
    }

    /// Returns `node_id`'s current simulated application incarnation.
    #[must_use]
    pub fn application_epoch(&self, node_id: NodeId) -> u64 {
        self.application_epochs
            .get(&node_id)
            .copied()
            .unwrap_or_default()
    }

    /// Returns the simulator-wide stream of applied application payloads.
    #[must_use]
    pub fn applied(&self) -> &[Applied] {
        &self.applied
    }

    /// Returns the promotion barrier currently required for `learner_id`.
    #[must_use]
    pub fn promotion_barrier(
        &self,
        node_id: NodeId,
        learner_id: NodeId,
    ) -> Option<PromotionBarrier> {
        self.node(node_id).promotion_barrier(learner_id)
    }

    /// Returns `node_id`'s committed index.
    #[must_use]
    pub fn commit_index(&self, node_id: NodeId) -> LogIndex {
        self.node(node_id).commit_index()
    }

    /// Returns `node_id`'s last local log index.
    #[must_use]
    pub fn last_log_index(&self, node_id: NodeId) -> LogIndex {
        self.node(node_id).last_log_index()
    }

    /// Returns `node_id`'s log entries starting at `first_index`.
    #[must_use]
    pub fn log_entries_from(&self, node_id: NodeId, first_index: LogIndex) -> Vec<LogEntry> {
        self.node(node_id).log_entries_from(first_index)
    }

    /// Returns `node_id`'s effective membership configuration.
    #[must_use]
    pub fn effective_membership(&self, node_id: NodeId) -> MembershipConfig {
        self.node(node_id).effective_membership()
    }

    /// Returns `node_id`'s committed membership configuration.
    #[must_use]
    pub fn committed_membership(&self, node_id: NodeId) -> MembershipConfig {
        self.node(node_id).committed_membership()
    }

    /// Returns `node_id`'s committed configuration identity, if known.
    #[must_use]
    pub fn committed_configuration_state(&self, node_id: NodeId) -> Option<CommittedConfiguration> {
        self.node(node_id).committed_configuration_state()
    }

    /// Returns a compact durable-state fingerprint for `node_id`.
    #[must_use]
    pub fn durable_state_digest(&self, node_id: NodeId) -> DurableStateDigest {
        let bootstrap = self.bootstrap_state(node_id);
        DurableStateDigest {
            current_term: bootstrap.current_term,
            voted_for: bootstrap.voted_for,
            commit_index: bootstrap.commit_index,
            committed_configuration: bootstrap.committed_configuration,
            snapshot: bootstrap
                .snapshot
                .as_ref()
                .map(|snapshot| DurableSnapshotDigest {
                    transfer_id: snapshot.transfer_id(),
                    last_included_index: snapshot.metadata.last_included_index,
                    last_included_term: snapshot.metadata.last_included_term,
                    hard_state_term: snapshot.metadata.hard_state_term,
                    application_payload_len: snapshot.application_payload_len,
                    application_payload_crc32: snapshot.application_payload_crc32,
                    committed_configuration: snapshot.metadata.committed_configuration_state(),
                }),
            log: bootstrap.log,
            application_epoch: self.application_epoch(node_id),
            applied_through: self.durable_applied_floor(node_id),
        }
    }

    /// Captures `node_id` as a restart bootstrap state.
    #[must_use]
    pub fn bootstrap_state(&self, node_id: NodeId) -> BootstrapState {
        let node = self.node(node_id);
        let first_log_index = node.snapshot_index().next();
        BootstrapState {
            current_term: node.current_term(),
            voted_for: node.voted_for(),
            commit_index: node.commit_index(),
            committed_configuration: node.committed_configuration_state(),
            snapshot: node.snapshot().cloned(),
            log: node
                .log_entries_from(first_log_index)
                .into_iter()
                .enumerate()
                .map(|(offset, entry)| BootstrapLogEntry {
                    index: LogIndex(first_log_index.0 + offset as u64),
                    term: entry.term,
                    kind: entry.kind,
                })
                .collect(),
        }
    }

    /// Returns whether `node_id` currently has an active read lease.
    #[must_use]
    pub fn read_lease_active(&self, node_id: NodeId) -> bool {
        self.node(node_id).read_lease_active()
    }

    /// Returns `node_id`'s current role.
    #[must_use]
    pub fn role(&self, node_id: NodeId) -> Role {
        self.node(node_id).role()
    }

    /// Returns `node_id`'s per-follower replication progress as reported by
    /// the kernel's leader observability; empty unless the node leads.
    #[must_use]
    pub fn leader_replication_progress(&self, node_id: NodeId) -> Vec<ReplicationProgress> {
        self.node(node_id).leader_replication_progress()
    }

    /// Returns `node_id`'s current term.
    #[must_use]
    pub fn current_term(&self, node_id: NodeId) -> Term {
        self.node(node_id).current_term()
    }

    pub(crate) fn node(&self, node_id: NodeId) -> &Node {
        self.nodes
            .get(&node_id)
            .expect("simulated node must exist in cluster")
    }

    pub(crate) fn node_mut(&mut self, node_id: NodeId) -> &mut Node {
        self.nodes
            .get_mut(&node_id)
            .expect("simulated node must exist in cluster")
    }
}
