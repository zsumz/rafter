//! Fixtures and filesystem paths shared by storage crash-window scenarios.

use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion, LogIndex,
    NodeId, RaftSnapshot, RaftSnapshotMetadata, SnapshotChunkRequest, SnapshotChunkSource,
    SnapshotGroupId, StagedSnapshotChunk, Term,
};

use crate::{PersistedRaftLogEntry, PersistedRaftSnapshot, RaftHardState, RaftSnapshotStore};

static WORKSPACE_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
pub(super) struct TestWorkspace {
    root: PathBuf,
}

impl TestWorkspace {
    pub(super) fn new(name: &str) -> Self {
        let id = WORKSPACE_ID.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "rafter-storage-crash-{name}-{}-{id}",
            std::process::id()
        ));
        fs::create_dir_all(&root).expect("crash-test workspace creates");
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

pub(super) fn initial_hard_state() -> RaftHardState {
    RaftHardState {
        current_term: Term(3),
        voted_for: Some(NodeId(2)),
        commit_index: LogIndex(4),
        committed_configuration: None,
    }
}

pub(super) fn replacement_hard_state() -> RaftHardState {
    RaftHardState {
        current_term: Term(7),
        voted_for: Some(NodeId(5)),
        commit_index: LogIndex(9),
        committed_configuration: None,
    }
}

pub(super) fn log_entries() -> Vec<PersistedRaftLogEntry> {
    vec![
        PersistedRaftLogEntry::application(LogIndex(1), Term(1), b"one".to_vec()),
        PersistedRaftLogEntry::application(LogIndex(2), Term(1), b"two".to_vec()),
        PersistedRaftLogEntry::application(LogIndex(3), Term(2), b"three".to_vec()),
    ]
}

pub(super) fn persisted_snapshot(index: u64, payload: &[u8]) -> PersistedRaftSnapshot {
    PersistedRaftSnapshot {
        metadata: RaftSnapshotMetadata::new(
            SnapshotGroupId::new("storage-crash-matrix").expect("group id is valid"),
            NodeId(1),
            LogIndex(index),
            Term(index),
            Term(index + 1),
            ApplicationSnapshotMetadata::new(
                ApplicationSnapshotKind::new("crash_matrix").expect("snapshot kind is valid"),
                ApplicationSnapshotVersion::new(1).expect("snapshot version is valid"),
            ),
        )
        .expect("snapshot metadata is valid"),
        application_payload: payload.to_vec(),
    }
}

pub(super) fn complete_staged_chunk(snapshot: &PersistedRaftSnapshot) -> StagedSnapshotChunk {
    let descriptor =
        RaftSnapshot::from_payload(snapshot.metadata.clone(), &snapshot.application_payload);
    StagedSnapshotChunk {
        leader_id: NodeId(9),
        transfer_id: descriptor.transfer_id(),
        metadata: descriptor.metadata.clone(),
        total_payload_len: descriptor.application_payload_len,
        application_payload_crc32: descriptor.application_payload_crc32,
        offset: 0,
        bytes: snapshot.application_payload.clone(),
        done: true,
    }
}

pub(super) fn assert_current_snapshot<S>(store: &S, expected: &PersistedRaftSnapshot)
where
    S: RaftSnapshotStore + SnapshotChunkSource,
{
    let descriptor =
        RaftSnapshot::from_payload(expected.metadata.clone(), &expected.application_payload);
    assert_eq!(store.current_snapshot(), Some(descriptor.clone()));
    assert_eq!(
        read_payload(store, &descriptor).as_slice(),
        expected.application_payload.as_slice()
    );
}

fn read_payload(source: &dyn SnapshotChunkSource, descriptor: &RaftSnapshot) -> Vec<u8> {
    const CHUNK_BYTES: u64 = 7;

    let mut payload = Vec::new();
    let mut offset = 0u64;
    while offset < descriptor.application_payload_len {
        let len = u32::try_from((descriptor.application_payload_len - offset).min(CHUNK_BYTES))
            .expect("bounded chunk length fits u32");
        let bytes = source
            .snapshot_chunk(SnapshotChunkRequest {
                transfer_id: descriptor.transfer_id(),
                metadata: &descriptor.metadata,
                total_payload_len: descriptor.application_payload_len,
                application_payload_crc32: descriptor.application_payload_crc32,
                offset,
                len,
            })
            .expect("current snapshot serves its descriptor");
        payload.extend_from_slice(&bytes);
        offset += u64::from(len);
    }
    payload
}

pub(super) fn hard_state_temp_path(path: &Path) -> PathBuf {
    let mut temp = path.as_os_str().to_os_string();
    temp.push(".tmp");
    PathBuf::from(temp)
}

pub(super) fn log_rewrite_temp_path(path: &Path) -> PathBuf {
    path.with_extension(format!("rewrite-{}.tmp", std::process::id()))
}

pub(super) fn log_marker_path(path: &Path) -> PathBuf {
    let mut marker = path.as_os_str().to_os_string();
    marker.push(".compact");
    PathBuf::from(marker)
}

pub(super) fn log_marker_temp_path(path: &Path) -> PathBuf {
    let mut temp = log_marker_path(path).into_os_string();
    temp.push(format!(".{}.tmp", std::process::id()));
    PathBuf::from(temp)
}
