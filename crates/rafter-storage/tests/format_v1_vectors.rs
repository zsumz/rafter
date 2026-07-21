//! Exact version-1 storage vectors.
//!
//! Round trips prove that each encoder agrees with its decoder. These fixtures
//! additionally pin the bytes selected by the public file-backed stores, so an
//! encoder and decoder cannot drift together unnoticed.

use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion,
    CommittedConfiguration, ConfigurationId, LogIndex, NodeId, RaftSnapshot, RaftSnapshotMetadata,
    SnapshotGroupId, StagedSnapshotChunk, Term,
};
use rafter_storage::{
    decode_raft_hard_state, decode_raft_log_entry, decode_raft_snapshot, encode_raft_hard_state,
    encode_raft_log_entry, encode_raft_snapshot, FileRaftHardStateStore, FileRaftLogSegment,
    FileRaftSnapshotStore, PersistedRaftLogEntry, PersistedRaftSnapshot, RaftHardState,
    RaftHardStateStore, RaftLogSegment, RaftSnapshotStore,
};

const HARD_STATE: &str = include_str!("vectors/v1/hard-state.hex");
const LOG_ENTRY: &str = include_str!("vectors/v1/log-entry-application.hex");
const LOG_FRAME: &str = include_str!("vectors/v1/log-frame-application.hex");
const LOG_COMPACTION: &str = include_str!("vectors/v1/log-compaction.hex");
const SNAPSHOT: &str = include_str!("vectors/v1/snapshot-empty.hex");
const SNAPSHOT_MANIFEST: &str = include_str!("vectors/v1/snapshot-manifest.hex");
const PENDING_TRANSFER: &str = include_str!("vectors/v1/pending-transfer.hex");
const PENDING_TRANSFER_BODY: &str = include_str!("vectors/v1/pending-transfer-body.hex");

static TEST_DIRECTORY_ID: AtomicU64 = AtomicU64::new(0);

#[test]
fn hard_state_writer_and_file_store_match_the_v1_vector() {
    let directory = TestDirectory::new("hard-state-vector");
    let path = directory.path().join("hard-state");
    let state = RaftHardState {
        current_term: Term(7),
        voted_for: Some(NodeId(3)),
        commit_index: LogIndex(11),
        committed_configuration: Some(CommittedConfiguration {
            index: LogIndex(9),
            config_id: ConfigurationId(4),
        }),
    };

    assert_eq!(
        decode_raft_hard_state(&decode_hex(HARD_STATE)),
        Ok(state),
        "the reference hard-state vector decodes"
    );
    assert_vector(
        "hard-state encoder",
        &encode_raft_hard_state(&state),
        HARD_STATE,
    );

    let mut store = FileRaftHardStateStore::open(&path).expect("hard-state store opens");
    store
        .write_hard_state(state)
        .expect("hard state writes durably");

    assert_vector(
        "hard-state file",
        &fs::read(path).expect("hard-state file reads"),
        HARD_STATE,
    );
}

#[test]
fn log_entry_frame_and_compaction_marker_match_the_v1_vectors() {
    let directory = TestDirectory::new("log-vectors");
    let path = directory.path().join("log");
    let entry = PersistedRaftLogEntry::application(LogIndex(1), Term(7), b"command".to_vec());

    assert_eq!(
        decode_raft_log_entry(&decode_hex(LOG_ENTRY)),
        Ok(entry.clone()),
        "the reference log-entry vector decodes"
    );
    assert_vector(
        "log-entry encoder",
        &encode_raft_log_entry(&entry).expect("log entry encodes"),
        LOG_ENTRY,
    );

    let mut segment = FileRaftLogSegment::open(&path).expect("log segment opens");
    segment
        .append_entries(std::slice::from_ref(&entry))
        .expect("log entry appends durably");
    assert_vector(
        "length-framed log file",
        &fs::read(&path).expect("log file reads"),
        LOG_FRAME,
    );

    segment
        .compact_prefix_through(LogIndex(1))
        .expect("log prefix compacts durably");
    assert!(
        fs::read(&path).expect("compacted log reads").is_empty(),
        "the sole compacted frame is reclaimed"
    );
    assert_vector(
        "log compaction marker",
        &fs::read(path_with_suffix(&path, ".compact")).expect("compaction marker reads"),
        LOG_COMPACTION,
    );
}

#[test]
fn snapshot_envelope_and_current_manifest_match_the_v1_vectors() {
    let directory = TestDirectory::new("snapshot-vectors");
    let snapshot_directory = directory.path().join("snapshots");
    let snapshot = PersistedRaftSnapshot {
        metadata: snapshot_metadata(NodeId(2), LogIndex(42), Term(7), Term(9)),
        application_payload: Vec::new(),
    };

    assert_eq!(
        decode_raft_snapshot(&decode_hex(SNAPSHOT)),
        Ok(snapshot.clone()),
        "the reference snapshot vector decodes"
    );
    assert_vector(
        "snapshot encoder",
        &encode_raft_snapshot(&snapshot).expect("snapshot encodes"),
        SNAPSHOT,
    );

    let mut store = FileRaftSnapshotStore::open(&snapshot_directory).expect("snapshot store opens");
    store
        .write_snapshot(snapshot)
        .expect("snapshot writes durably");

    let file_name = store
        .current_snapshot_file_name()
        .expect("current snapshot has a file name");
    assert_eq!(file_name, "snapshot-1-42-7-2.rfsn");
    assert_vector(
        "snapshot file",
        &fs::read(snapshot_directory.join(file_name)).expect("snapshot file reads"),
        SNAPSHOT,
    );
    assert_vector(
        "current snapshot manifest",
        &fs::read(snapshot_directory.join("current.snapshot"))
            .expect("current snapshot manifest reads"),
        SNAPSHOT_MANIFEST,
    );
}

#[test]
fn pending_transfer_manifest_and_body_match_the_v1_vectors() {
    let directory = TestDirectory::new("pending-transfer-vectors");
    let snapshot_directory = directory.path().join("snapshots");
    let mut store = FileRaftSnapshotStore::open(&snapshot_directory).expect("snapshot store opens");
    let metadata = snapshot_metadata(NodeId(1), LogIndex(7), Term(6), Term(6));
    let transfer_id = RaftSnapshot::new(metadata.clone(), 64, 0).transfer_id();
    let chunk = StagedSnapshotChunk {
        leader_id: NodeId(1),
        transfer_id,
        metadata,
        total_payload_len: 64,
        application_payload_crc32: 0,
        offset: 0,
        bytes: b"partial body".to_vec(),
        done: false,
    };

    store
        .stage_snapshot_chunk(&chunk)
        .expect("pending snapshot chunk stages durably");

    assert_vector(
        "pending transfer manifest",
        &fs::read(snapshot_directory.join("pending.snapshot-transfer"))
            .expect("pending transfer manifest reads"),
        PENDING_TRANSFER,
    );
    assert_vector(
        "pending transfer body",
        &fs::read(snapshot_directory.join("pending.snapshot-transfer.body"))
            .expect("pending transfer body reads"),
        PENDING_TRANSFER_BODY,
    );
}

fn snapshot_metadata(
    writer_id: NodeId,
    last_included_index: LogIndex,
    last_included_term: Term,
    hard_state_term: Term,
) -> RaftSnapshotMetadata {
    RaftSnapshotMetadata::new(
        SnapshotGroupId::new("data-group-10").expect("snapshot group id is valid"),
        writer_id,
        last_included_index,
        last_included_term,
        hard_state_term,
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("stream_data")
                .expect("application snapshot kind is valid"),
            ApplicationSnapshotVersion::new(1).expect("application snapshot version is valid"),
        ),
    )
    .expect("snapshot metadata is valid")
}

fn assert_vector(name: &str, actual: &[u8], vector: &str) {
    let expected = decode_hex(vector);
    assert_eq!(
        actual,
        expected.as_slice(),
        "{name} changed its version-1 bytes"
    );
}

fn decode_hex(source: &str) -> Vec<u8> {
    source
        .lines()
        .flat_map(|line| {
            line.split('#')
                .next()
                .unwrap_or_default()
                .split_ascii_whitespace()
        })
        .map(|byte| u8::from_str_radix(byte, 16).expect("vector contains hexadecimal bytes"))
        .collect()
}

fn path_with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut suffixed = path.as_os_str().to_os_string();
    suffixed.push(suffix);
    PathBuf::from(suffixed)
}

struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new(name: &str) -> Self {
        let id = TEST_DIRECTORY_ID.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("rafter-storage-{name}-{}-{id}", std::process::id()));
        fs::create_dir_all(&path).expect("test directory creates");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}
