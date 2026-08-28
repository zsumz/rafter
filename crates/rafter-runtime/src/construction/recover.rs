//! Restart constructors that hand back committed replay outputs.
//!
//! These open storage exactly as the plain constructors do, then drain what
//! the recovered kernel has committed above the applied floor and return it
//! beside the node. The split is the point: an embedder cannot reach the node
//! without deciding what to do with the outputs its own state machine missed.

use rafter::{LogIndex, NodeConfig as RaftNodeConfig};
use rafter_storage::{RaftHardStateStore, RaftLogSegment, RaftSnapshotStore};

use crate::{DurableRaftNode, RaftRuntimeError, RecoveredDurableRaftNode};

impl<H: RaftHardStateStore, L: RaftLogSegment, S: RaftSnapshotStore> DurableRaftNode<H, L, S> {
    /// Recovers a deterministic Raft node and returns any committed
    /// application outputs above the default applied floor.
    ///
    /// Prefer this constructor for datastore restart paths when the caller
    /// must replay committed entries immediately after opening durable
    /// storage.
    ///
    /// # Errors
    ///
    /// Returns [`RaftRuntimeError::Bootstrap`] when persisted hard state,
    /// snapshot metadata, or the retained log suffix is invalid for the
    /// supplied configuration.
    pub fn recover_with_storage_and_snapshot_store(
        config: RaftNodeConfig,
        hard_state_store: H,
        log_segment: L,
        snapshot_store: S,
    ) -> Result<RecoveredDurableRaftNode<H, L, S>, RaftRuntimeError> {
        Self::recover_with_storage_and_snapshot_store_applied_through(
            config,
            hard_state_store,
            log_segment,
            snapshot_store,
            LogIndex::ZERO,
        )
    }

    /// Recovers a deterministic Raft node and returns committed application
    /// outputs above the supplied applied floor.
    ///
    /// The returned [`RecoveredDurableRaftNode`] must be split with
    /// [`RecoveredDurableRaftNode::into_parts`], making the recovery outputs
    /// an explicit part of the construction flow. This is the safer default
    /// for datastore embedders that persist their state-machine applied index
    /// separately from Raft hard state.
    ///
    /// ```
    /// use rafter::{Input, LogIndex, NodeConfig, NodeId, Output};
    /// use rafter_runtime::DurableRaftNode;
    /// use rafter_storage::InMemoryRaftHardStateStore;
    ///
    /// let config = NodeConfig::new(NodeId(1), Vec::new(), 3).expect("valid raft config");
    /// let mut node = DurableRaftNode::new(config.clone(), InMemoryRaftHardStateStore::new())
    ///     .expect("a fresh in-memory node opens");
    /// for _ in 0..3 {
    ///     node.step(Input::Tick).expect("each tick persists what it changed");
    /// }
    /// for command in [&b"set alpha=one"[..], &b"set beta=two"[..]] {
    ///     node.step(Input::ClientProposal {
    ///         payload: command.to_vec(),
    ///     })
    ///     .expect("the lone voter commits its own proposal");
    /// }
    /// let storage = node.into_storage();
    ///
    /// // Restart. The application declares how far its own durable state
    /// // reached; the runtime replays what is above that floor and nothing at
    /// // or below it, so a command already applied cannot be applied twice.
    /// let recovered = DurableRaftNode::recover_with_storage_and_snapshot_store_applied_through(
    ///     config,
    ///     storage.hard_state_store,
    ///     storage.log_segment,
    ///     storage.snapshot_store,
    ///     LogIndex(2),
    /// )
    /// .expect("the durable state is valid for this configuration");
    /// let (node, recovery_outputs) = recovered.into_parts();
    ///
    /// let replayed = recovery_outputs
    ///     .iter()
    ///     .filter_map(|output| match output {
    ///         Output::Apply { index, .. } => Some(*index),
    ///         _ => None,
    ///     })
    ///     .collect::<Vec<_>>();
    /// assert_eq!(replayed, vec![LogIndex(3)]);
    /// assert_eq!(node.applied_index(), LogIndex(3));
    /// ```
    ///
    /// # Errors
    ///
    /// As the plain constructor, plus a bootstrap error when the floor lies
    /// beyond the persisted log.
    pub fn recover_with_storage_and_snapshot_store_applied_through(
        config: RaftNodeConfig,
        hard_state_store: H,
        log_segment: L,
        snapshot_store: S,
        applied_through: LogIndex,
    ) -> Result<RecoveredDurableRaftNode<H, L, S>, RaftRuntimeError> {
        let mut node = Self::with_storage_and_snapshot_store_applied_through(
            config,
            hard_state_store,
            log_segment,
            snapshot_store,
            applied_through,
        )?;
        let recovery_outputs = node.drain_committed_outputs();
        Ok(RecoveredDurableRaftNode {
            node,
            recovery_outputs,
        })
    }
}
