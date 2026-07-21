//! Fixtures and state comparisons shared by storage conformance traces.

use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion, LogIndex,
    NodeId, RaftSnapshot, RaftSnapshotMetadata, SnapshotChunkRequest, SnapshotChunkSource,
    SnapshotGroupId, StagedSnapshotChunk, Term,
};
use rafter_storage::{
    FileRaftLogSegment, FileRaftSnapshotStore, InMemoryRaftLogSegment, InMemoryRaftSnapshotStore,
    PersistedRaftSnapshot, RaftLogSegment, RaftSnapshotStore,
};

static WORKSPACE_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
pub(super) struct TestWorkspace {
    root: PathBuf,
}

impl TestWorkspace {
    pub(super) fn new(name: &str) -> Self {
        let id = WORKSPACE_ID.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "rafter-storage-conformance-{name}-{}-{id}",
            std::process::id()
        ));
        fs::create_dir_all(&root).expect("conformance workspace creates");
        Self { root }
    }

    pub(super) fn path(&self, name: &str) -> PathBuf {
        self.root.join(name)
    }
}

impl Drop for TestWorkspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

pub(super) fn assert_log_equivalent(memory: &InMemoryRaftLogSegment, file: &FileRaftLogSegment) {
    assert_eq!(memory.replay_entries(), file.replay_entries());
    assert_eq!(memory.next_index(), file.next_index());
    assert_eq!(memory.compacted_through(), file.compacted_through());
    assert!(!file.requires_reopen());
}

pub(super) fn persisted_snapshot(
    index: u64,
    term: u64,
    hard_state_term: u64,
    payload: &[u8],
) -> PersistedRaftSnapshot {
    PersistedRaftSnapshot {
        metadata: RaftSnapshotMetadata::new(
            SnapshotGroupId::new("storage-conformance").expect("group id is valid"),
            NodeId(1),
            LogIndex(index),
            Term(term),
            Term(hard_state_term),
            ApplicationSnapshotMetadata::new(
                ApplicationSnapshotKind::new("conformance").expect("snapshot kind is valid"),
                ApplicationSnapshotVersion::new(1).expect("snapshot version is valid"),
            ),
        )
        .expect("snapshot metadata is valid"),
        application_payload: payload.to_vec(),
    }
}

pub(super) fn staged_chunks(
    snapshot: &PersistedRaftSnapshot,
    split_at: usize,
) -> (RaftSnapshot, StagedSnapshotChunk, StagedSnapshotChunk) {
    assert!(split_at > 0 && split_at < snapshot.application_payload.len());
    let descriptor =
        RaftSnapshot::from_payload(snapshot.metadata.clone(), &snapshot.application_payload);
    let leader_id = NodeId(9);
    let first = staged_chunk(
        &descriptor,
        leader_id,
        0,
        snapshot.application_payload[..split_at].to_vec(),
        false,
    );
    let final_chunk = staged_chunk(
        &descriptor,
        leader_id,
        u64::try_from(split_at).expect("snapshot split offset fits u64"),
        snapshot.application_payload[split_at..].to_vec(),
        true,
    );
    (descriptor, first, final_chunk)
}

pub(super) fn staged_chunk(
    descriptor: &RaftSnapshot,
    leader_id: NodeId,
    offset: u64,
    bytes: Vec<u8>,
    done: bool,
) -> StagedSnapshotChunk {
    StagedSnapshotChunk {
        leader_id,
        transfer_id: descriptor.transfer_id(),
        metadata: descriptor.metadata.clone(),
        total_payload_len: descriptor.application_payload_len,
        application_payload_crc32: descriptor.application_payload_crc32,
        offset,
        bytes,
        done,
    }
}

pub(super) fn assert_snapshot_equivalent(
    memory: &InMemoryRaftSnapshotStore,
    file: &FileRaftSnapshotStore,
) {
    let memory_current = memory.current_snapshot();
    let file_current = file.current_snapshot();
    assert_eq!(memory_current, file_current);
    assert_eq!(
        memory.current_pending_snapshot_transfer(),
        file.current_pending_snapshot_transfer()
    );
    assert!(!file.requires_reopen());

    if let Some(descriptor) = memory_current {
        assert_eq!(
            read_payload(memory, &descriptor),
            read_payload(file, &descriptor)
        );
    }
}

fn read_payload(source: &dyn SnapshotChunkSource, descriptor: &RaftSnapshot) -> Vec<u8> {
    const CHUNK_BYTES: u64 = 7;

    let mut payload = Vec::new();
    let mut offset = 0u64;
    while offset < descriptor.application_payload_len {
        let len = u32::try_from((descriptor.application_payload_len - offset).min(CHUNK_BYTES))
            .expect("bounded conformance chunk length fits u32");
        let bytes = source
            .snapshot_chunk(SnapshotChunkRequest {
                transfer_id: descriptor.transfer_id(),
                metadata: &descriptor.metadata,
                total_payload_len: descriptor.application_payload_len,
                application_payload_crc32: descriptor.application_payload_crc32,
                offset,
                len,
            })
            .expect("snapshot source serves its current descriptor");
        assert_eq!(
            bytes.len(),
            usize::try_from(len).expect("conformance chunk length fits usize")
        );
        payload.extend_from_slice(&bytes);
        offset += u64::from(len);
    }
    payload
}
