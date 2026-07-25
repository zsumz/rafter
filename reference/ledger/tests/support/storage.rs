//! A snapshot store that keeps its medium behind a guard.
//!
//! Hard state and the log are plain in-memory stores here: `into_storage`
//! returns every store when an incarnation is retired, so the driver never
//! needs a second handle to a medium it is not currently driving.
//!
//! The snapshot store is deliberately not plain. It is the one store whose
//! staging read cannot lend a borrow of store-owned state — a pooled, locked,
//! or lazily loading store has nothing to borrow from — and holding its medium
//! behind a `RefCell` is how this consumer keeps
//! [`RaftSnapshotStore::current_pending_snapshot_transfer`] honest about
//! returning an owned value. A store shaped like this needed a cached mirror of
//! durable staging before that method returned owned; it needs nothing now.

use std::cell::RefCell;
use std::rc::Rc;

use rafter::{
    PendingSnapshotTransfer, RaftSnapshot, SnapshotChunkRequest, SnapshotChunkSource,
    StagedSnapshotChunk,
};
use rafter_storage::{
    InMemoryRaftSnapshotStore, PersistedRaftSnapshot, RaftSnapshotStore,
    RaftSnapshotStoreWriteError,
};

/// Handle to one replica's durable snapshot medium.
///
/// Every read answers from the medium rather than from a field, so a second
/// handle to the same medium can never observe staging the first one changed.
#[derive(Clone, Debug, Default)]
pub struct SharedSnapshotStore {
    medium: Rc<RefCell<InMemoryRaftSnapshotStore>>,
}

impl RaftSnapshotStore for SharedSnapshotStore {
    fn write_snapshot(
        &mut self,
        snapshot: PersistedRaftSnapshot,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        self.medium.borrow_mut().write_snapshot(snapshot)
    }

    fn write_snapshot_from_source(
        &mut self,
        snapshot: &RaftSnapshot,
        source: &dyn SnapshotChunkSource,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        self.medium
            .borrow_mut()
            .write_snapshot_from_source(snapshot, source)
    }

    fn current_snapshot(&self) -> Option<RaftSnapshot> {
        self.medium.borrow().current_snapshot()
    }

    fn stage_snapshot_chunk(
        &mut self,
        chunk: &StagedSnapshotChunk,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        self.medium.borrow_mut().stage_snapshot_chunk(chunk)
    }

    fn promote_staged_snapshot(
        &mut self,
        snapshot: &RaftSnapshot,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        self.medium.borrow_mut().promote_staged_snapshot(snapshot)
    }

    fn clear_pending_snapshot_transfer(&mut self) -> Result<(), RaftSnapshotStoreWriteError> {
        self.medium.borrow_mut().clear_pending_snapshot_transfer()
    }

    fn current_pending_snapshot_transfer(&self) -> Option<PendingSnapshotTransfer> {
        self.medium.borrow().current_pending_snapshot_transfer()
    }
}

impl SnapshotChunkSource for SharedSnapshotStore {
    fn snapshot_chunk(&self, request: SnapshotChunkRequest<'_>) -> Option<Vec<u8>> {
        self.medium.borrow().snapshot_chunk(request)
    }
}
