//! Runtime-owned snapshot retention over the file-backed store.

use std::fs;

use rafter::{
    Input as RaftInput, LogIndex, Output as RaftOutput, SnapshotChunkRequest, SnapshotChunkSource,
};
use rafter_invariant_test::{oracle_assert, oracle_assert_eq};
use rafter_storage::{
    FileRaftHardStateStore, FileRaftLogSegment, FileRaftNodeStores, FileRaftSnapshotStore,
    RaftSnapshotStore, SnapshotRetention,
};

use super::*;
use crate::tests::file_backed_fixture::TestDirectory;

type FileNode = DurableRaftNode<FileRaftHardStateStore, FileRaftLogSegment, FileRaftSnapshotStore>;

fn open_node(directory: &TestDirectory) -> FileNode {
    let (hard_state, log, snapshots) = FileRaftNodeStores::open(directory.path())
        .expect("file stores open")
        .into_parts();
    DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(1, &[]),
        hard_state,
        log,
        snapshots,
    )
    .expect("single-voter runtime opens")
}

fn commit(runtime: &mut FileNode, payload: &[u8], index: u64) {
    let outputs = runtime
        .step(RaftInput::ClientProposal {
            payload: payload.to_vec(),
        })
        .expect("single-voter proposal persists and commits");
    oracle_assert!(outputs.iter().any(
        |output| matches!(output, RaftOutput::Apply { index: applied, .. } if *applied == LogIndex(index))
    ));
}

#[test]
fn runtime_prunes_only_noncurrent_snapshot_history_and_reopens_from_current() {
    let directory = TestDirectory::new("snapshot-maintenance");
    let unknown = directory.path().join("snapshots/operator-note.txt");
    let mut runtime = open_node(&directory);
    oracle_assert!(runtime
        .step(RaftInput::Tick)
        .expect("single voter elects")
        .is_empty());

    commit(&mut runtime, b"first", 2);
    runtime
        .compact_log_with_snapshot(raft_snapshot_for_writer(
            2,
            1,
            1,
            1,
            b"application through two",
        ))
        .expect("first snapshot publishes and compacts");
    commit(&mut runtime, b"second", 3);
    runtime
        .compact_log_with_snapshot(raft_snapshot_for_writer(
            3,
            1,
            1,
            1,
            b"application through three",
        ))
        .expect("second snapshot publishes and compacts");
    fs::write(&unknown, b"operator owned").expect("unknown file writes");

    let before = runtime
        .snapshot_store()
        .snapshot_inventory()
        .expect("snapshot inventory is authoritative");
    oracle_assert_eq!(before.retained.len(), 1);
    oracle_assert_eq!(
        before
            .current
            .as_ref()
            .and_then(|file| file.identity)
            .map(|identity| identity.last_included_index),
        Some(LogIndex(3))
    );

    let report = runtime
        .prune_snapshot_files(SnapshotRetention::CurrentOnly)
        .expect("noncurrent snapshot prunes durably");
    oracle_assert_eq!(report.removed_snapshots.len(), 1);
    oracle_assert!(report.removed_temporary_files.is_empty());

    let abandoned = directory
        .path()
        .join(format!("snapshots/.snapshot-{}.tmp", std::process::id()));
    fs::write(&abandoned, b"interrupted publication").expect("abandoned temp writes");
    let cleanup = runtime
        .cleanup_abandoned_snapshot_temporary_files()
        .expect("abandoned snapshot temp cleans durably");
    oracle_assert!(cleanup.removed_snapshots.is_empty());
    oracle_assert_eq!(cleanup.removed_temporary_files.len(), 1);
    oracle_assert!(!abandoned.exists());

    let current = runtime
        .snapshot()
        .expect("current descriptor remains")
        .clone();
    let bytes = runtime
        .snapshot_store()
        .snapshot_chunk(SnapshotChunkRequest {
            transfer_id: current.transfer_id(),
            metadata: &current.metadata,
            total_payload_len: current.application_payload_len,
            application_payload_crc32: current.application_payload_crc32,
            offset: 0,
            len: u32::try_from(current.application_payload_len)
                .expect("test snapshot length fits u32"),
        })
        .expect("current payload remains serviceable");
    oracle_assert_eq!(bytes, b"application through three".to_vec());
    oracle_assert!(unknown.is_file());

    let after = runtime
        .snapshot_store()
        .snapshot_inventory()
        .expect("post-prune inventory succeeds");
    oracle_assert!(after.retained.is_empty());
    oracle_assert!(after.unreferenced.is_empty());
    oracle_assert!(after.temporary.is_empty());
    oracle_assert_eq!(after.unrecognized, vec!["operator-note.txt".to_string()]);

    drop(runtime);
    let reopened = open_node(&directory);
    oracle_assert_eq!(reopened.snapshot_index(), LogIndex(3));
    oracle_assert_eq!(reopened.commit_index(), LogIndex(3));
    oracle_assert_eq!(reopened.last_log_index(), LogIndex(3));
    oracle_assert_eq!(
        reopened
            .snapshot_store()
            .current_snapshot()
            .expect("selected snapshot reopens"),
        current
    );
}
