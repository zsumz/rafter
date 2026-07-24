//! Durable media handles that outlive one node incarnation.
//!
//! `DurableRaftNode` takes ownership of its stores and never hands them back,
//! so an in-process restart needs a handle the driver still holds. Each handle
//! here shares one in-memory medium the way a file-backed store shares one
//! directory: dropping the node leaves the medium intact, and the next
//! incarnation reopens exactly the state the previous one persisted.
//!
//! The media are behind `Arc<Mutex<_>>` rather than a single-threaded cell
//! because `rafter-service` requires its command sender to be `Send + Sync`,
//! and the sender owns the group that owns these stores.
//!
//! Snapshots are deliberately absent. This slice's state machine refuses to
//! build a durable application snapshot, so nothing ever compacts and every
//! incarnation opens an empty snapshot store. Durable snapshot media arrive
//! with the durable slice.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use rafter::LogIndex;
use rafter_storage::{
    InMemoryRaftHardStateStore, InMemoryRaftLogSegment, PersistedRaftLogEntry, RaftHardState,
    RaftHardStateStore, RaftHardStateStoreWriteError, RaftLogSegment, RaftLogSegmentAppendError,
    RaftLogSegmentCompactError, RaftLogSegmentTruncateError,
};

/// Every durable store belonging to one replica.
#[derive(Clone, Debug, Default)]
pub struct NodeStorage {
    pub hard_state: SharedHardStateStore,
    pub log: SharedLogSegment,
}

impl NodeStorage {
    /// Creates empty durable storage for a replica that has never started.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns handles to the same durable media for a new node incarnation.
    pub fn reopen(&self) -> Self {
        self.clone()
    }
}

/// Shared handle to one replica's durable hard state.
#[derive(Clone, Debug, Default)]
pub struct SharedHardStateStore {
    medium: Arc<Mutex<InMemoryRaftHardStateStore>>,
}

impl RaftHardStateStore for SharedHardStateStore {
    fn write_hard_state(
        &mut self,
        state: RaftHardState,
    ) -> Result<(), RaftHardStateStoreWriteError> {
        lock(&self.medium).write_hard_state(state)
    }

    fn current(&self) -> RaftHardState {
        lock(&self.medium).current()
    }
}

/// Shared handle to one replica's durable log segment.
#[derive(Clone, Debug, Default)]
pub struct SharedLogSegment {
    medium: Arc<Mutex<InMemoryRaftLogSegment>>,
}

impl RaftLogSegment for SharedLogSegment {
    fn append_entries(
        &mut self,
        entries: &[PersistedRaftLogEntry],
    ) -> Result<(), RaftLogSegmentAppendError> {
        lock(&self.medium).append_entries(entries)
    }

    fn truncate_suffix(&mut self, from_index: LogIndex) -> Result<(), RaftLogSegmentTruncateError> {
        lock(&self.medium).truncate_suffix(from_index)
    }

    fn compact_prefix_through(
        &mut self,
        through_index: LogIndex,
    ) -> Result<(), RaftLogSegmentCompactError> {
        lock(&self.medium).compact_prefix_through(through_index)
    }

    fn replay_entries(&self) -> Vec<PersistedRaftLogEntry> {
        lock(&self.medium).replay_entries()
    }

    fn next_index(&self) -> LogIndex {
        lock(&self.medium).next_index()
    }

    fn compacted_through(&self) -> LogIndex {
        lock(&self.medium).compacted_through()
    }
}

/// A poisoned medium still holds exactly the bytes the last successful write
/// left there, which is what a durable medium is supposed to do after a crash.
fn lock<T>(medium: &Mutex<T>) -> MutexGuard<'_, T> {
    medium.lock().unwrap_or_else(PoisonError::into_inner)
}
