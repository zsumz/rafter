//! Constructors that supply the durable stores a caller left out.
//!
//! Each one fills the missing halves of the durable triple with in-memory
//! stores and delegates onward, so a short call and a fully specified one
//! hydrate through exactly the same path. Nothing is decided here that the
//! hydration path does not decide again.

use rafter::{NodeConfig as RaftNodeConfig, RaftSnapshotMetadata};
use rafter_storage::{
    InMemoryRaftLogSegment, InMemoryRaftSnapshotStore, PersistedRaftSnapshot, RaftHardStateStore,
    RaftLogSegment,
};

use crate::{DurableRaftNode, RaftRuntimeError};

impl<H: RaftHardStateStore> DurableRaftNode<H, InMemoryRaftLogSegment, InMemoryRaftSnapshotStore> {
    /// Hydrates a deterministic Raft node from the supplied hard-state store.
    ///
    /// # Errors
    ///
    /// Returns [`RaftRuntimeError::Bootstrap`] when the persisted hard state is
    /// invalid for the supplied configuration.
    pub fn new(config: RaftNodeConfig, hard_state_store: H) -> Result<Self, RaftRuntimeError> {
        Self::with_storage(config, hard_state_store, InMemoryRaftLogSegment::new())
    }
}

impl<H: RaftHardStateStore, L: RaftLogSegment> DurableRaftNode<H, L, InMemoryRaftSnapshotStore> {
    /// Hydrates a deterministic Raft node from supplied hard-state and log
    /// stores.
    ///
    /// # Errors
    ///
    /// Returns [`RaftRuntimeError::Bootstrap`] when the persisted hard state or
    /// log is invalid for the supplied configuration.
    pub fn with_storage(
        config: RaftNodeConfig,
        hard_state_store: H,
        log_segment: L,
    ) -> Result<Self, RaftRuntimeError> {
        Self::with_storage_and_snapshot_store(
            config,
            hard_state_store,
            log_segment,
            InMemoryRaftSnapshotStore::new(),
        )
    }

    /// Hydrates a deterministic Raft node from supplied hard-state, retained
    /// log, and optional durable snapshot metadata.
    ///
    /// This metadata-only convenience constructor uses an empty application
    /// snapshot payload. Production snapshot recovery should prefer
    /// [`Self::with_storage_and_snapshot_store`] so leaders can send complete
    /// install-snapshot messages.
    ///
    /// # Errors
    ///
    /// Returns [`RaftRuntimeError::Bootstrap`] when persisted hard state,
    /// snapshot metadata, or the retained log suffix is invalid for the
    /// supplied configuration.
    pub fn with_storage_and_snapshot(
        config: RaftNodeConfig,
        hard_state_store: H,
        log_segment: L,
        snapshot: Option<RaftSnapshotMetadata>,
    ) -> Result<Self, RaftRuntimeError> {
        let snapshot_store = snapshot.map_or_else(InMemoryRaftSnapshotStore::new, |metadata| {
            InMemoryRaftSnapshotStore::with_snapshot(PersistedRaftSnapshot {
                metadata,
                application_payload: Vec::new(),
            })
        });
        Self::with_storage_and_snapshot_store(config, hard_state_store, log_segment, snapshot_store)
    }
}
