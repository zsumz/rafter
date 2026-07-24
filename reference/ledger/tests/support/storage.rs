//! Durable-medium handles that outlive one node incarnation.
//!
//! `DurableRaftNode` takes ownership of its stores and never hands them back,
//! so an in-process restart needs a store handle the driver still holds. Each
//! handle here shares one in-memory store the same way a file-backed store
//! shares one directory: dropping the node leaves the medium intact, and the
//! next incarnation reopens exactly the state the previous one persisted.

use std::cell::RefCell;
use std::rc::Rc;

use rafter::{
    LogIndex, PendingSnapshotTransfer, RaftSnapshot, SnapshotChunkRequest, SnapshotChunkSource,
    StagedSnapshotChunk,
};
use rafter_storage::{
    InMemoryRaftHardStateStore, InMemoryRaftLogSegment, InMemoryRaftSnapshotStore,
    PersistedRaftLogEntry, PersistedRaftSnapshot, RaftHardState, RaftHardStateStore,
    RaftHardStateStoreWriteError, RaftLogSegment, RaftLogSegmentAppendError,
    RaftLogSegmentCompactError, RaftLogSegmentTruncateError, RaftSnapshotStore,
    RaftSnapshotStoreWriteError,
};

/// Every durable store belonging to one replica.
#[derive(Clone, Debug, Default)]
pub struct NodeStorage {
    pub hard_state: SharedHardStateStore,
    pub log: SharedLogSegment,
    pub snapshots: SharedSnapshotStore,
}

impl NodeStorage {
    /// Creates empty durable storage for a replica that has never started.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns handles to the same durable media for a new node incarnation.
    pub fn reopen(&self) -> Self {
        Self {
            hard_state: self.hard_state.clone(),
            log: self.log.clone(),
            snapshots: self.snapshots.reopen(),
        }
    }
}

/// Shared handle to one replica's durable hard state.
#[derive(Clone, Debug, Default)]
pub struct SharedHardStateStore {
    medium: Rc<RefCell<InMemoryRaftHardStateStore>>,
}

impl RaftHardStateStore for SharedHardStateStore {
    fn write_hard_state(
        &mut self,
        state: RaftHardState,
    ) -> Result<(), RaftHardStateStoreWriteError> {
        self.medium.borrow_mut().write_hard_state(state)
    }

    fn current(&self) -> RaftHardState {
        self.medium.borrow().current()
    }
}

/// Shared handle to one replica's durable log segment.
#[derive(Clone, Debug, Default)]
pub struct SharedLogSegment {
    medium: Rc<RefCell<InMemoryRaftLogSegment>>,
}

impl RaftLogSegment for SharedLogSegment {
    fn append_entries(
        &mut self,
        entries: &[PersistedRaftLogEntry],
    ) -> Result<(), RaftLogSegmentAppendError> {
        self.medium.borrow_mut().append_entries(entries)
    }

    fn truncate_suffix(&mut self, from_index: LogIndex) -> Result<(), RaftLogSegmentTruncateError> {
        self.medium.borrow_mut().truncate_suffix(from_index)
    }

    fn compact_prefix_through(
        &mut self,
        through_index: LogIndex,
    ) -> Result<(), RaftLogSegmentCompactError> {
        self.medium
            .borrow_mut()
            .compact_prefix_through(through_index)
    }

    fn replay_entries(&self) -> Vec<PersistedRaftLogEntry> {
        self.medium.borrow().replay_entries()
    }

    fn next_index(&self) -> LogIndex {
        self.medium.borrow().next_index()
    }

    fn compacted_through(&self) -> LogIndex {
        self.medium.borrow().compacted_through()
    }
}

/// Shared handle to one replica's durable snapshot store.
///
/// `RaftSnapshotStore::current_pending_snapshot_transfer` returns a borrow of
/// store-owned state, which no handle over shared or lazily loaded state can
/// produce. This handle therefore mirrors the staged transfer locally and
/// refreshes the mirror after every mutation that can change it.
#[derive(Clone, Debug, Default)]
pub struct SharedSnapshotStore {
    medium: Rc<RefCell<InMemoryRaftSnapshotStore>>,
    pending_mirror: Option<PendingSnapshotTransfer>,
}

impl SharedSnapshotStore {
    /// Returns a handle to the same medium with a freshly read staging mirror.
    pub fn reopen(&self) -> Self {
        let mut reopened = self.clone();
        reopened.refresh_pending_mirror();
        reopened
    }

    fn refresh_pending_mirror(&mut self) {
        self.pending_mirror = self
            .medium
            .borrow()
            .current_pending_snapshot_transfer()
            .cloned();
    }
}

impl RaftSnapshotStore for SharedSnapshotStore {
    fn write_snapshot(
        &mut self,
        snapshot: PersistedRaftSnapshot,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        let result = self.medium.borrow_mut().write_snapshot(snapshot);
        self.refresh_pending_mirror();
        result
    }

    fn write_snapshot_from_source(
        &mut self,
        snapshot: &RaftSnapshot,
        source: &dyn SnapshotChunkSource,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        let result = self
            .medium
            .borrow_mut()
            .write_snapshot_from_source(snapshot, source);
        self.refresh_pending_mirror();
        result
    }

    fn current_snapshot(&self) -> Option<RaftSnapshot> {
        self.medium.borrow().current_snapshot()
    }

    fn stage_snapshot_chunk(
        &mut self,
        chunk: &StagedSnapshotChunk,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        let result = self.medium.borrow_mut().stage_snapshot_chunk(chunk);
        self.refresh_pending_mirror();
        result
    }

    fn promote_staged_snapshot(
        &mut self,
        snapshot: &RaftSnapshot,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        let result = self.medium.borrow_mut().promote_staged_snapshot(snapshot);
        self.refresh_pending_mirror();
        result
    }

    fn clear_pending_snapshot_transfer(&mut self) -> Result<(), RaftSnapshotStoreWriteError> {
        let result = self.medium.borrow_mut().clear_pending_snapshot_transfer();
        self.refresh_pending_mirror();
        result
    }

    fn current_pending_snapshot_transfer(&self) -> Option<&PendingSnapshotTransfer> {
        self.pending_mirror.as_ref()
    }
}

impl SnapshotChunkSource for SharedSnapshotStore {
    fn snapshot_chunk(&self, request: SnapshotChunkRequest<'_>) -> Option<Vec<u8>> {
        self.medium.borrow().snapshot_chunk(request)
    }
}
