//! Publication-before-compaction failures preserve durable recovery authority.

use super::{
    super::*,
    support::{adapt, config, log_with_commands, Store},
};
use crate::{DurableRaftNode, DurableRaftNodeStorage, RaftRuntimeError};
use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion, Input,
    LogIndex, RaftSnapshotMetadata, SnapshotGroupId,
};
use rafter_storage::{
    InMemoryRaftLogSegment, InMemoryRaftSnapshotStore, PersistedRaftLogEntry,
    PersistedRaftSnapshot, RaftLogSegment, RaftLogSegmentAppendError, RaftLogSegmentCompactError,
    RaftLogSegmentTruncateError, RaftSnapshotStore,
};
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
struct FailCompact(InMemoryRaftLogSegment);
impl RaftLogSegment for FailCompact {
    fn append_entries(
        &mut self,
        entries: &[PersistedRaftLogEntry],
    ) -> Result<(), RaftLogSegmentAppendError> {
        self.0.append_entries(entries)
    }
    fn truncate_suffix(&mut self, from: LogIndex) -> Result<(), RaftLogSegmentTruncateError> {
        self.0.truncate_suffix(from)
    }
    fn compact_prefix_through(&mut self, _: LogIndex) -> Result<(), RaftLogSegmentCompactError> {
        Err(RaftLogSegmentCompactError::Io {
            operation: "injected compact failure",
            source: std::io::Error::other("failed after snapshot publication").into(),
        })
    }
    fn replay_entries(&self) -> Vec<PersistedRaftLogEntry> {
        self.0.replay_entries()
    }
    fn next_index(&self) -> LogIndex {
        self.0.next_index()
    }
    fn compacted_through(&self) -> LogIndex {
        self.0.compacted_through()
    }
}

#[test]
fn failed_log_compaction_poison_survives_already_published_snapshot() {
    let storage = log_with_commands(3).into_storage();
    let mut owner = DurableService::recover(
        config(),
        DurableRaftNodeStorage {
            hard_state_store: storage.hard_state_store,
            log_segment: FailCompact(storage.log_segment),
            snapshot_store: storage.snapshot_store,
        },
        Store::default(),
        ServiceOptions::new(),
        adapt,
    )
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while owner.state() != ServiceState::Ready {
        assert!(Instant::now() < deadline);
        owner.poll().unwrap();
        std::thread::yield_now();
    }
    let result = owner.compact_snapshot_with(|application, node| {
        let at = application.floor;
        let metadata = RaftSnapshotMetadata::new(
            SnapshotGroupId::new("failed-compaction")?,
            node.id(),
            at,
            node.term_at_index(at).unwrap(),
            node.current_term(),
            ApplicationSnapshotMetadata::new(
                ApplicationSnapshotKind::new("index")?,
                ApplicationSnapshotVersion::new(1)?,
            ),
        )?;
        let source = InMemoryRaftSnapshotStore::with_snapshot(PersistedRaftSnapshot {
            metadata,
            application_payload: at.0.to_le_bytes().to_vec(),
        });
        Ok((source.current_snapshot().unwrap(), source))
    });
    assert!(matches!(
        result,
        Err(ServiceError::Snapshot(RaftRuntimeError::LogCompact(_)))
    ));
    assert_eq!(owner.state(), ServiceState::Failed);
    assert!(owner.try_step(Input::Tick.into()).is_err());
    assert!(owner.poll().is_err());
    let storage = owner.ready_node().unwrap().clone().into_storage();
    assert_eq!(
        storage
            .snapshot_store
            .current_snapshot()
            .unwrap()
            .metadata
            .last_included_index,
        LogIndex(4)
    );
    assert_eq!(storage.log_segment.compacted_through(), LogIndex::ZERO);
    let (reopened, outputs) =
        DurableRaftNode::recover_with_storage_and_snapshot_store_applied_through(
            config(),
            storage.hard_state_store,
            storage.log_segment.0,
            storage.snapshot_store,
            LogIndex(4),
        )
        .unwrap()
        .into_parts();
    assert_eq!(reopened.snapshot_index(), LogIndex(4));
    assert_eq!(reopened.commit_index(), LogIndex(4));
    assert!(outputs.is_empty());
}
