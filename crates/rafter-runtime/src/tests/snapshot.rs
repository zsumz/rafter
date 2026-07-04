use super::*;
use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion,
    InMemorySnapshotChunkSource, PendingSnapshotTransfer, RaftSnapshotMetadata, SnapshotGroupId,
    StagedSnapshotChunk,
};
use rafter_storage::{
    RaftLogSegmentAppendError, RaftLogSegmentCompactError, RaftLogSegmentTruncateError,
};

mod bootstrap_compaction;
mod chunk_transfer;
mod install;

#[derive(Clone, Debug, Eq, PartialEq)]
struct FailingCompactRaftLogSegment {
    entries: Vec<PersistedRaftLogEntry>,
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

pub(super) fn persisted_entry(index: u64, term: u64, payload: &[u8]) -> PersistedRaftLogEntry {
    PersistedRaftLogEntry::application(LogIndex(index), Term(term), payload.to_vec())
}

fn snapshot_metadata(index: u64, term: u64, hard_state_term: u64) -> RaftSnapshotMetadata {
    RaftSnapshotMetadata::new(
        SnapshotGroupId::new("data-group-10").expect("valid group id"),
        RaftNodeId(2),
        LogIndex(index),
        Term(term),
        Term(hard_state_term),
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("stream_data").expect("valid kind"),
            ApplicationSnapshotVersion::new(1).expect("valid version"),
        ),
    )
    .expect("snapshot metadata is valid")
}

fn compacted_leader_snapshot() -> PersistedRaftSnapshot {
    let create_payload = b"create stream payload".to_vec();
    let append_payload = b"append stream payload".to_vec();
    let snapshot = raft_snapshot_for_writer(3, 1, 1, 2, b"opaque application snapshot");
    let mut leader = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[]),
        InMemoryRaftHardStateStore::new(),
        InMemoryRaftLogSegment::new(),
        InMemoryRaftSnapshotStore::new(),
    )
    .expect("leader hydrates");

    assert!(leader
        .step(RaftInput::Tick)
        .expect("leader elects")
        .is_empty());
    assert_committed(&mut leader, create_payload, LogIndex(2), "create commits");
    assert_committed(&mut leader, append_payload, LogIndex(3), "append commits");
    assert_eq!(leader.commit_index(), LogIndex(3));

    leader
        .compact_log_with_snapshot(snapshot.clone())
        .expect("leader has durable snapshot before compacting prefix");
    assert_eq!(leader.snapshot_index(), LogIndex(3));
    assert_eq!(leader.log_segment.replay_entries(), Vec::new());
    leader
        .snapshot_store
        .current()
        .cloned()
        .expect("runtime writes a normalized local snapshot")
}

#[test]
fn compact_log_with_streamed_snapshot_persists_compacts_and_installs_descriptor() {
    let snapshot = raft_snapshot_for_writer(3, 1, 1, 2, b"streamed application snapshot");
    let descriptor =
        RaftSnapshot::from_payload(snapshot.metadata.clone(), &snapshot.application_payload);
    let mut source = InMemorySnapshotChunkSource::new();
    source
        .insert(&descriptor, snapshot.application_payload.clone())
        .expect("source holds the streamed payload");
    let mut leader = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[]),
        InMemoryRaftHardStateStore::new(),
        InMemoryRaftLogSegment::new(),
        InMemoryRaftSnapshotStore::new(),
    )
    .expect("leader hydrates");
    assert!(leader
        .step(RaftInput::Tick)
        .expect("leader elects")
        .is_empty());
    assert_committed(
        &mut leader,
        b"create stream payload".to_vec(),
        LogIndex(2),
        "create commits",
    );
    assert_committed(
        &mut leader,
        b"append stream payload".to_vec(),
        LogIndex(3),
        "append commits",
    );

    leader
        .compact_log_with_streamed_snapshot(descriptor.clone(), &source)
        .expect("leader compacts through the streamed snapshot");

    assert_eq!(leader.snapshot_index(), LogIndex(3));
    assert_eq!(leader.log_segment.replay_entries(), Vec::new());
    let installed = leader.snapshot().expect("snapshot descriptor installs");
    assert_eq!(
        installed.metadata.committed_membership(),
        Some(&MembershipConfig::stable(membership_set(&[2])))
    );
    assert_eq!(
        leader.snapshot_store.current(),
        Some(&PersistedRaftSnapshot {
            metadata: installed.metadata.clone(),
            application_payload: snapshot.application_payload,
        })
    );
}

fn assert_committed(
    leader: &mut DurableRaftNode,
    payload: Vec<u8>,
    index: LogIndex,
    context: &'static str,
) {
    assert!(leader
        .step(RaftInput::ClientProposal { payload })
        .expect(context)
        .iter()
        .any(
            |output| matches!(output, RaftOutput::Apply { index: applied, .. } if *applied == index)
        ));
}

fn stale_snapshot_follower() -> DurableRaftNode {
    let mut follower_log = InMemoryRaftLogSegment::new();
    follower_log
        .append_entries(&[persisted_entry(1, 1, b"old prefix")])
        .expect("behind follower has stale prefix");
    DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(3, &[2]),
        hard_state_store(1, None),
        follower_log,
        InMemoryRaftSnapshotStore::new(),
    )
    .expect("behind follower restarts")
}

fn restart_snapshot_follower(follower: &DurableRaftNode) -> DurableRaftNode {
    DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(3, &[2]),
        follower.hard_state_store.clone(),
        follower.log_segment.clone(),
        follower.snapshot_store.clone(),
    )
    .expect("follower restarts from durable stores")
}

fn install_snapshot_chunk(
    follower: &mut DurableRaftNode,
    snapshot: &PersistedRaftSnapshot,
    transfer_id: rafter::SnapshotTransferId,
    offset: usize,
    end: usize,
) -> Vec<RaftOutput> {
    follower
        .step(RaftInput::Message {
            from: RaftNodeId(2),
            message: Message::InstallSnapshotChunk(rafter::InstallSnapshotChunk {
                term: Term(1),
                leader_id: RaftNodeId(2),
                transfer_id,
                metadata: snapshot.metadata.clone(),
                total_payload_len: snapshot.application_payload.len() as u64,
                application_payload_crc32: rafter_storage::crc32(&snapshot.application_payload),
                offset: offset as u64,
                chunk: snapshot.application_payload[offset..end].to_vec(),
                done: end == snapshot.application_payload.len(),
            }),
        })
        .expect("snapshot chunk persists before response")
}

fn assert_partial_snapshot_transfer(
    follower: &DurableRaftNode,
    outputs: &[RaftOutput],
    received_bytes: u64,
) {
    assert!(
        outputs
            .iter()
            .all(|output| !matches!(output, RaftOutput::ApplySnapshot { .. })),
        "follower must not apply or claim caught up after a partial snapshot"
    );
    assert_eq!(follower.snapshot_index(), LogIndex::ZERO);
    assert_eq!(follower.snapshot_store.current_snapshot(), None);
    assert_eq!(
        follower
            .snapshot_transfer_status()
            .follower
            .expect("partial transfer is visible")
            .received_bytes,
        received_bytes
    );
}

fn applied_snapshot_from(outputs: &[RaftOutput]) -> &RaftSnapshot {
    outputs
        .iter()
        .find_map(|output| match output {
            RaftOutput::ApplySnapshot { snapshot } => Some(snapshot),
            _ => None,
        })
        .expect("final chunk applies snapshot")
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FailingSnapshotStore;

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

fn snapshot_transfer_id(
    metadata: &RaftSnapshotMetadata,
    total_payload_len: u64,
) -> rafter::SnapshotTransferId {
    RaftSnapshot::new(metadata.clone(), total_payload_len, 0).transfer_id()
}

pub(super) fn raft_snapshot(
    index: u64,
    term: u64,
    hard_state_term: u64,
    payload: &[u8],
) -> PersistedRaftSnapshot {
    PersistedRaftSnapshot {
        metadata: snapshot_metadata(index, term, hard_state_term),
        application_payload: payload.to_vec(),
    }
}

fn raft_snapshot_for_writer(
    index: u64,
    term: u64,
    hard_state_term: u64,
    writer_id: u64,
    payload: &[u8],
) -> PersistedRaftSnapshot {
    PersistedRaftSnapshot {
        metadata: RaftSnapshotMetadata::new(
            SnapshotGroupId::new("data-group-10").expect("valid group id"),
            RaftNodeId(writer_id),
            LogIndex(index),
            Term(term),
            Term(hard_state_term),
            ApplicationSnapshotMetadata::new(
                ApplicationSnapshotKind::new("stream_data").expect("valid kind"),
                ApplicationSnapshotVersion::new(1).expect("valid version"),
            ),
        )
        .expect("snapshot metadata is valid"),
        application_payload: payload.to_vec(),
    }
}
