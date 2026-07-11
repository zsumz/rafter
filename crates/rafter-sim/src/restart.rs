use rafter::{BootstrapState, BootstrapValidationError, LogIndex, Node, NodeId};

use crate::Cluster;

impl Cluster {
    /// Replaces a simulated node with a freshly hydrated Raft node.
    ///
    /// This models a process restart at the deterministic protocol boundary:
    /// hard state and log entries come from the supplied bootstrap state while
    /// volatile Raft state such as role, commit index, and in-flight progress is
    /// reset. The node's durable snapshot payload store survives the restart;
    /// its volatile staging area for a partially received transfer does not.
    ///
    /// # Errors
    ///
    /// Returns [`BootstrapValidationError`] when the supplied state is not
    /// valid for the node's static configuration.
    ///
    /// # Panics
    ///
    /// Panics when `node_id` is not part of this simulated cluster, or when
    /// the bootstrap state carries a snapshot descriptor whose payload was
    /// never registered in the node's snapshot store (see
    /// [`Cluster::seed_snapshot_payload`]): durable metadata without durable
    /// content models a persistence bug, not a restart.
    pub fn restart_node_from_bootstrap(
        &mut self,
        node_id: NodeId,
        bootstrap: BootstrapState,
    ) -> Result<(), BootstrapValidationError> {
        let config = self
            .configs
            .get(&node_id)
            .expect("simulated node config must exist in cluster")
            .clone();
        if let Some(snapshot) = bootstrap.snapshot.as_ref() {
            assert!(
                self.snapshot_payload(node_id, snapshot).is_some(),
                "snapshot payload for transfer {} must be seeded in {node_id}'s snapshot store \
                 before restarting with its descriptor",
                snapshot.transfer_id()
            );
        }
        let mut node = Node::from_bootstrap_applied_through(
            config,
            bootstrap,
            self.durable_applied_floor(node_id),
        )?;
        let outputs = node.drain_committed_outputs();
        self.nodes.insert(node_id, node);
        // A process restart loses the volatile staging area; explicit resume
        // paths (the model checker's durable-transfer restart) reinstate it
        // alongside the kernel's pending-transfer record.
        self.snapshot_staging.remove(&node_id);
        self.record_outputs(node_id, outputs);
        Ok(())
    }

    pub(crate) fn durable_applied_floor(&self, node_id: NodeId) -> LogIndex {
        self.durable_applied
            .get(&node_id)
            .copied()
            .unwrap_or(LogIndex::ZERO)
    }

    /// Restarts `node_id` from `bootstrap` after losing the simulated
    /// application state for that node.
    ///
    /// This is for durability-violation and storage-repair scenarios. Normal
    /// process restarts should use [`Cluster::restart_node_from_bootstrap`],
    /// which preserves the simulator's durable application floor.
    ///
    /// # Errors
    ///
    /// Returns [`BootstrapValidationError`] when the supplied state is not
    /// valid for the node's static configuration.
    ///
    /// # Panics
    ///
    /// Panics when `node_id` is not part of this simulated cluster, or when
    /// the bootstrap state carries a snapshot descriptor whose payload was
    /// never registered in the node's snapshot store.
    pub fn restart_node_from_bootstrap_losing_application_state(
        &mut self,
        node_id: NodeId,
        bootstrap: BootstrapState,
    ) -> Result<(), BootstrapValidationError> {
        let config = self
            .configs
            .get(&node_id)
            .expect("simulated node config must exist in cluster")
            .clone();
        if let Some(snapshot) = bootstrap.snapshot.as_ref() {
            assert!(
                self.snapshot_payload(node_id, snapshot).is_some(),
                "snapshot payload for transfer {} must be seeded in {node_id}'s snapshot store \
                 before restarting with its descriptor",
                snapshot.transfer_id()
            );
        }
        let snapshot_boundary = bootstrap
            .snapshot
            .as_ref()
            .map_or(LogIndex::ZERO, |snapshot| {
                snapshot.metadata.last_included_index
            });

        let mut node = Node::from_bootstrap_applied_through(config, bootstrap, snapshot_boundary)?;
        let outputs = node.drain_committed_outputs();
        self.nodes.insert(node_id, node);
        self.snapshot_staging.remove(&node_id);
        self.durable_applied.insert(node_id, snapshot_boundary);
        self.record_outputs(node_id, outputs);
        Ok(())
    }

    /// Captures this node's current bootstrap state as its "last synced"
    /// point for [`Cluster::restart_node_from_mark`].
    ///
    /// The caller owns legality: restarting from a mark that predates entries
    /// the node has acknowledged models a durability violation, which the
    /// leader's match floor deliberately never rewinds until a protocol-level
    /// rejection hint can prove the follower's lower durable tail.
    pub fn mark_synced(&mut self, node_id: NodeId) {
        let mark = self.bootstrap_state(node_id);
        self.synced_marks.insert(node_id, mark);
    }

    /// Restarts `node_id` from its [`Cluster::mark_synced`] point with its
    /// LIVE term/vote and lost application state — a crash that lost the log
    /// tail written after the mark while term and vote survived. This is the
    /// explicit amnesia model for assumption-violating durability tests;
    /// ordinary restarts preserve the simulator's durable application floor.
    ///
    /// # Panics
    ///
    /// Panics when no mark was captured or the composed state fails
    /// bootstrap validation.
    pub fn restart_node_from_mark(&mut self, node_id: NodeId) {
        let mark = self
            .synced_marks
            .get(&node_id)
            .cloned()
            .expect("a synced mark must be captured before a marked restart");
        let live = self.bootstrap_state(node_id);
        let bootstrap = BootstrapState {
            current_term: live.current_term,
            voted_for: live.voted_for,
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
            snapshot: mark.snapshot,
            log: mark.log,
        };
        self.restart_node_from_bootstrap_losing_application_state(node_id, bootstrap)
            .expect("marked lossy restart composes a valid bootstrap state");
    }

    /// Restarts `node_id` losing exactly the log tail no leader ever heard
    /// acknowledged and that the node has not locally committed: the log
    /// rewinds to the delivered-acknowledgement floor, the local commit floor,
    /// or the snapshot boundary, whichever is highest. Hard state, the local
    /// commit floor, the committed configuration identity, and the snapshot
    /// survive. Legal by construction — every lost entry's acknowledgement
    /// envelope was still in flight or dropped, and no local committed prefix
    /// is erased — so schedules may apply it freely without weakening the
    /// safety invariants.
    ///
    /// # Panics
    ///
    /// Panics when the truncated state fails bootstrap validation, which
    /// would be a harness bug.
    pub fn restart_node_lossy(&mut self, node_id: NodeId) {
        let live = self.bootstrap_state(node_id);
        let floor = self
            .delivered_ack_floor
            .get(&node_id)
            .copied()
            .unwrap_or(LogIndex::ZERO)
            .max(live.commit_index)
            .max(live.snapshot.as_ref().map_or(LogIndex::ZERO, |snapshot| {
                snapshot.metadata.last_included_index
            }));
        let bootstrap = BootstrapState {
            current_term: live.current_term,
            voted_for: live.voted_for,
            commit_index: live.commit_index,
            committed_configuration: live.committed_configuration,
            snapshot: live.snapshot,
            log: live
                .log
                .into_iter()
                .filter(|entry| entry.index <= floor)
                .collect(),
        };
        self.restart_node_from_bootstrap(node_id, bootstrap)
            .expect("floor-truncated lossy restart composes a valid bootstrap state");
    }

    /// The highest match index `node_id` has confirmed to any leader via a
    /// DELIVERED acknowledgement.
    #[must_use]
    pub fn delivered_ack_floor(&self, node_id: NodeId) -> LogIndex {
        self.delivered_ack_floor
            .get(&node_id)
            .copied()
            .unwrap_or(LogIndex::ZERO)
    }
}
