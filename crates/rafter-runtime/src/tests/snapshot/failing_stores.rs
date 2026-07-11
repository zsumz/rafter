use super::*;
use rafter::{PendingSnapshotTransfer, StagedSnapshotChunk};
use rafter_storage::{
    RaftLogSegmentAppendError, RaftLogSegmentCompactError, RaftLogSegmentTruncateError,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct FailingCompactRaftLogSegment {
    pub(super) entries: Vec<PersistedRaftLogEntry>,
}

impl RaftLogSegment for FailingCompactRaftLogSegment {
    fn append_entries(
        &mut self,
        entries: &[PersistedRaftLogEntry],
    ) -> Result<(), RaftLogSegmentAppendError> {
        self.entries.extend_from_slice(entries);
        Ok(())
    }

    fn truncate_suffix(&mut self, from_index: LogIndex) -> Result<(), RaftLogSegmentTruncateError> {
        self.entries.retain(|entry| entry.index < from_index);
        Ok(())
    }

    fn compact_prefix_through(
        &mut self,
        _through_index: LogIndex,
    ) -> Result<(), RaftLogSegmentCompactError> {
        Err(RaftLogSegmentCompactError::Io {
            operation: "compact test raft log entries",
            message: "injected failure".to_string(),
        })
    }

    fn replay_entries(&self) -> Vec<PersistedRaftLogEntry> {
        self.entries.clone()
    }

    fn next_index(&self) -> LogIndex {
        self.entries
            .last()
            .map_or(LogIndex(1), |entry| entry.index.next())
    }

    fn compacted_through(&self) -> LogIndex {
        LogIndex::ZERO
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct FailingSnapshotStore;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct FailingPromoteSnapshotStore(pub(super) InMemoryRaftSnapshotStore);

impl RaftSnapshotStore for FailingSnapshotStore {
    fn write_snapshot(
        &mut self,
        _snapshot: PersistedRaftSnapshot,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        Err(RaftSnapshotStoreWriteError::Io {
            operation: "write test raft snapshot",
            path: std::path::PathBuf::from("test-snapshot"),
            message: "injected failure".to_string(),
        })
    }

    fn write_snapshot_from_source(
        &mut self,
        _snapshot: &RaftSnapshot,
        _source: &dyn SnapshotChunkSource,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        Err(RaftSnapshotStoreWriteError::Io {
            operation: "write test raft snapshot",
            path: std::path::PathBuf::from("test-snapshot"),
            message: "injected failure".to_string(),
        })
    }

    fn current_snapshot(&self) -> Option<RaftSnapshot> {
        None
    }

    fn stage_snapshot_chunk(
        &mut self,
        _chunk: &StagedSnapshotChunk,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        Err(RaftSnapshotStoreWriteError::Io {
            operation: "stage test snapshot chunk",
            path: std::path::PathBuf::from("test-pending-snapshot-transfer"),
            message: "injected failure".to_string(),
        })
    }

    fn promote_staged_snapshot(
        &mut self,
        _snapshot: &RaftSnapshot,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        Err(RaftSnapshotStoreWriteError::Io {
            operation: "promote test staged snapshot",
            path: std::path::PathBuf::from("test-pending-snapshot-transfer"),
            message: "injected failure".to_string(),
        })
    }

    fn clear_pending_snapshot_transfer(&mut self) -> Result<(), RaftSnapshotStoreWriteError> {
        Ok(())
    }

    fn current_pending_snapshot_transfer(&self) -> Option<&PendingSnapshotTransfer> {
        None
    }
}

impl SnapshotChunkSource for FailingSnapshotStore {
    fn snapshot_chunk(&self, _request: rafter::SnapshotChunkRequest<'_>) -> Option<Vec<u8>> {
        None
    }
}

impl RaftSnapshotStore for FailingPromoteSnapshotStore {
    fn write_snapshot(
        &mut self,
        snapshot: PersistedRaftSnapshot,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        self.0.write_snapshot(snapshot)
    }

    fn write_snapshot_from_source(
        &mut self,
        snapshot: &RaftSnapshot,
        source: &dyn SnapshotChunkSource,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        self.0.write_snapshot_from_source(snapshot, source)
    }

    fn current_snapshot(&self) -> Option<RaftSnapshot> {
        self.0.current_snapshot()
    }

    fn stage_snapshot_chunk(
        &mut self,
        chunk: &StagedSnapshotChunk,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        self.0.stage_snapshot_chunk(chunk)
    }

    fn promote_staged_snapshot(
        &mut self,
        _snapshot: &RaftSnapshot,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        Err(RaftSnapshotStoreWriteError::Io {
            operation: "promote test staged snapshot",
            path: std::path::PathBuf::from("test-pending-snapshot-transfer"),
            message: "injected failure".to_string(),
        })
    }

    fn clear_pending_snapshot_transfer(&mut self) -> Result<(), RaftSnapshotStoreWriteError> {
        self.0.clear_pending_snapshot_transfer()
    }

    fn current_pending_snapshot_transfer(&self) -> Option<&PendingSnapshotTransfer> {
        self.0.current_pending_snapshot_transfer()
    }
}

impl SnapshotChunkSource for FailingPromoteSnapshotStore {
    fn snapshot_chunk(&self, request: rafter::SnapshotChunkRequest<'_>) -> Option<Vec<u8>> {
        self.0.snapshot_chunk(request)
    }
}
