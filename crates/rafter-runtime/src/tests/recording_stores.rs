//! Durable stores that journal every mutation they are asked to perform.
//!
//! These exist for assertions about what a runtime operation does *not* write.
//! A journal shared by all three stores lets one test distinguish "wrote
//! nothing" from "wrote to a store this test forgot to watch".

use std::{cell::RefCell, rc::Rc};

use super::*;

/// Mutations observed across one replica's durable stores, in call order.
#[derive(Clone, Debug, Default)]
pub(super) struct StoreJournal {
    entries: Rc<RefCell<Vec<&'static str>>>,
}

impl StoreJournal {
    pub(super) fn new() -> Self {
        Self::default()
    }

    pub(super) fn entries(&self) -> Vec<&'static str> {
        self.entries.borrow().clone()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.entries.borrow().is_empty()
    }

    fn record(&self, operation: &'static str) {
        self.entries.borrow_mut().push(operation);
    }
}

/// Hard-state store that journals its writes.
#[derive(Clone, Debug)]
pub(super) struct RecordingHardStateStore {
    journal: StoreJournal,
    inner: InMemoryRaftHardStateStore,
}

impl RecordingHardStateStore {
    pub(super) fn new(journal: &StoreJournal, inner: InMemoryRaftHardStateStore) -> Self {
        Self {
            journal: journal.clone(),
            inner,
        }
    }
}

impl RaftHardStateStore for RecordingHardStateStore {
    fn write_hard_state(
        &mut self,
        state: RaftHardState,
    ) -> Result<(), RaftHardStateStoreWriteError> {
        self.journal.record("write_hard_state");
        self.inner.write_hard_state(state)
    }

    fn current(&self) -> RaftHardState {
        self.inner.current()
    }
}

/// Log segment that journals its mutations.
#[derive(Clone, Debug)]
pub(super) struct RecordingLogSegment {
    journal: StoreJournal,
    inner: InMemoryRaftLogSegment,
}

impl RecordingLogSegment {
    pub(super) fn new(journal: &StoreJournal, inner: InMemoryRaftLogSegment) -> Self {
        Self {
            journal: journal.clone(),
            inner,
        }
    }
}

impl RaftLogSegment for RecordingLogSegment {
    fn append_entries(
        &mut self,
        entries: &[PersistedRaftLogEntry],
    ) -> Result<(), RaftLogSegmentAppendError> {
        self.journal.record("append_entries");
        self.inner.append_entries(entries)
    }

    fn truncate_suffix(&mut self, from_index: LogIndex) -> Result<(), RaftLogSegmentTruncateError> {
        self.journal.record("truncate_suffix");
        self.inner.truncate_suffix(from_index)
    }

    fn compact_prefix_through(
        &mut self,
        through_index: LogIndex,
    ) -> Result<(), RaftLogSegmentCompactError> {
        self.journal.record("compact_prefix_through");
        self.inner.compact_prefix_through(through_index)
    }

    fn replay_entries(&self) -> Vec<PersistedRaftLogEntry> {
        self.inner.replay_entries()
    }

    fn next_index(&self) -> LogIndex {
        self.inner.next_index()
    }

    fn compacted_through(&self) -> LogIndex {
        self.inner.compacted_through()
    }
}

/// Snapshot store that journals its mutations.
#[derive(Clone, Debug)]
pub(super) struct RecordingSnapshotStore {
    journal: StoreJournal,
    inner: InMemoryRaftSnapshotStore,
}

impl RecordingSnapshotStore {
    pub(super) fn new(journal: &StoreJournal, inner: InMemoryRaftSnapshotStore) -> Self {
        Self {
            journal: journal.clone(),
            inner,
        }
    }
}

impl RaftSnapshotStore for RecordingSnapshotStore {
    fn write_snapshot(
        &mut self,
        snapshot: PersistedRaftSnapshot,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        self.journal.record("write_snapshot");
        self.inner.write_snapshot(snapshot)
    }

    fn write_snapshot_from_source(
        &mut self,
        snapshot: &RaftSnapshot,
        source: &dyn SnapshotChunkSource,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        self.journal.record("write_snapshot_from_source");
        self.inner.write_snapshot_from_source(snapshot, source)
    }

    fn current_snapshot(&self) -> Option<RaftSnapshot> {
        self.inner.current_snapshot()
    }

    fn stage_snapshot_chunk(
        &mut self,
        chunk: &rafter::StagedSnapshotChunk,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        self.journal.record("stage_snapshot_chunk");
        self.inner.stage_snapshot_chunk(chunk)
    }

    fn promote_staged_snapshot(
        &mut self,
        snapshot: &RaftSnapshot,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        self.journal.record("promote_staged_snapshot");
        self.inner.promote_staged_snapshot(snapshot)
    }

    fn clear_pending_snapshot_transfer(&mut self) -> Result<(), RaftSnapshotStoreWriteError> {
        self.journal.record("clear_pending_snapshot_transfer");
        self.inner.clear_pending_snapshot_transfer()
    }

    fn current_pending_snapshot_transfer(&self) -> Option<rafter::PendingSnapshotTransfer> {
        self.inner.current_pending_snapshot_transfer()
    }
}

impl SnapshotChunkSource for RecordingSnapshotStore {
    fn snapshot_chunk(&self, request: SnapshotChunkRequest<'_>) -> Option<Vec<u8>> {
        self.inner.snapshot_chunk(request)
    }
}
