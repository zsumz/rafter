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
fn retired_snapshot_prefix_obeys_worker_credits_and_shutdown() {
    let directory = TestDirectory::new("snapshot-retirement-worker");
    let mut runtime = open_node(&directory);
    oracle_assert!(runtime
        .step(RaftInput::Tick)
        .expect("single voter elects")
        .is_empty());
    commit(&mut runtime, b"first", 2);
    commit(&mut runtime, b"second", 3);

    let retired = runtime
        .compact_log_with_snapshot_deferred_drop(raft_snapshot_for_writer(
            2,
            1,
            1,
            1,
            b"application through two",
        ))
        .expect("the first snapshot publishes and returns its retired prefix");
    oracle_assert_eq!(retired.len(), 2);
    oracle_assert_eq!(retired.payload_bytes(), b"first".len());
    oracle_assert_eq!(runtime.snapshot_index(), LogIndex(2));
    oracle_assert_eq!(runtime.last_log_index(), LogIndex(3));

    let mut undersized = LogRetirementWorker::start(LogRetirementWorkerOptions::new(1, 1024))
        .expect("the undersized worker starts");
    let retired = undersized
        .try_submit(retired)
        .expect_err("entry credits refuse the whole prefix")
        .into_retired_entries();
    oracle_assert_eq!(retired.len(), 2);
    undersized.shutdown().expect("the idle worker joins");

    let mut worker = LogRetirementWorker::start(LogRetirementWorkerOptions::new(2, 1024))
        .expect("the adequately sized worker starts");
    worker
        .try_submit(retired)
        .expect("the whole prefix transfers without waiting");
    worker.shutdown().expect("shutdown drains accepted work");
    oracle_assert_eq!(worker.inflight_entries(), 0);
    oracle_assert_eq!(worker.inflight_payload_bytes(), 0);

    let retired = runtime
        .compact_log_with_snapshot_deferred_drop(raft_snapshot_for_writer(
            3,
            1,
            1,
            1,
            b"application through three",
        ))
        .expect("the second snapshot publishes and returns its retired prefix");
    oracle_assert_eq!(retired.len(), 1);
    let retired = worker
        .try_submit(retired)
        .expect_err("a stopped worker returns ownership")
        .into_retired_entries();
    oracle_assert_eq!(retired.len(), 1);
    drop(retired);

    drop(runtime);
    let reopened = open_node(&directory);
    oracle_assert_eq!(reopened.snapshot_index(), LogIndex(3));
    oracle_assert_eq!(reopened.commit_index(), LogIndex(3));
    oracle_assert_eq!(reopened.last_log_index(), LogIndex(3));
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
