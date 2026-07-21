//! Snapshot publication and pending-cleanup windows reconstruct authoritative state.

use crate::{FileRaftSnapshotStore, RaftSnapshotStore, RaftSnapshotStoreWriteError};

use super::{
    arm,
    support_test::{
        assert_current_snapshot, complete_staged_chunk, persisted_snapshot, TestWorkspace,
    },
    DurabilityPoint,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReopenedSnapshot {
    Absent,
    Current,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CleanupResidue {
    AbandonedBody,
    NoFiles,
}

#[test]
fn snapshot_publication_matrix_reopens_from_the_current_manifest() {
    let cases = [
        (
            DurabilityPoint::SnapshotAfterTempSync,
            ReopenedSnapshot::Absent,
        ),
        (
            DurabilityPoint::SnapshotAfterFileRename,
            ReopenedSnapshot::Absent,
        ),
        (
            DurabilityPoint::SnapshotAfterFileDirectorySync,
            ReopenedSnapshot::Absent,
        ),
        (
            DurabilityPoint::SnapshotAfterManifestTempSync,
            ReopenedSnapshot::Absent,
        ),
        (
            DurabilityPoint::SnapshotAfterManifestRename,
            ReopenedSnapshot::Current,
        ),
        (
            DurabilityPoint::SnapshotAfterManifestDirectorySync,
            ReopenedSnapshot::Current,
        ),
    ];

    for (point, expected) in cases {
        verify_snapshot_publication_window(point, expected);
    }
}

fn verify_snapshot_publication_window(point: DurabilityPoint, expected: ReopenedSnapshot) {
    let workspace = TestWorkspace::new(&format!("snapshot-publish-{point:?}"));
    let directory = workspace.path("snapshots");
    let snapshot = persisted_snapshot(5, b"snapshot publication payload");
    let mut store = FileRaftSnapshotStore::open(&directory).expect("snapshot store opens");

    let guard = arm(point);
    let error = store
        .write_snapshot(snapshot.clone())
        .expect_err("armed publication point fails");
    guard.assert_triggered();

    assert!(matches!(error, RaftSnapshotStoreWriteError::Io { .. }));
    assert!(store.requires_reopen());
    assert!(
        store.current_snapshot().is_none(),
        "the poisoned handle reports only acknowledged current state"
    );

    drop(store);
    let reopened = FileRaftSnapshotStore::open(&directory).expect("snapshot store reopens");
    assert!(!reopened.requires_reopen());
    match expected {
        ReopenedSnapshot::Absent => assert!(reopened.current_snapshot().is_none()),
        ReopenedSnapshot::Current => assert_current_snapshot(&reopened, &snapshot),
    }
}

#[test]
fn committed_snapshot_cleanup_matrix_preserves_the_new_current_snapshot() {
    let cases = [
        (
            DurabilityPoint::PendingClearAfterManifestRemoval,
            CleanupResidue::AbandonedBody,
        ),
        (
            DurabilityPoint::PendingClearAfterBodyRemoval,
            CleanupResidue::NoFiles,
        ),
        (
            DurabilityPoint::PendingClearAfterDirectorySync,
            CleanupResidue::NoFiles,
        ),
    ];

    for (point, expected_residue) in cases {
        verify_committed_cleanup_window(point, expected_residue);
    }
}

fn verify_committed_cleanup_window(point: DurabilityPoint, expected_residue: CleanupResidue) {
    let workspace = TestWorkspace::new(&format!("snapshot-cleanup-{point:?}"));
    let directory = workspace.path("snapshots");
    let staged = persisted_snapshot(5, b"staged incoming snapshot");
    let replacement = persisted_snapshot(8, b"new current snapshot");
    let mut store = FileRaftSnapshotStore::open(&directory).expect("snapshot store opens");
    store
        .stage_snapshot_chunk(&complete_staged_chunk(&staged))
        .expect("complete pending transfer stages");

    let guard = arm(point);
    let error = store
        .write_snapshot(replacement.clone())
        .expect_err("cleanup fails after current manifest publication");
    guard.assert_triggered();

    assert!(matches!(
        error,
        RaftSnapshotStoreWriteError::SnapshotCommittedButReopenRequired { .. }
    ));
    assert!(store.requires_reopen());
    assert_current_snapshot(&store, &replacement);
    assert!(store.current_pending_snapshot_transfer().is_some());

    drop(store);
    let reopened = FileRaftSnapshotStore::open(&directory).expect("snapshot store reopens");
    assert_current_snapshot(&reopened, &replacement);
    assert!(reopened.current_pending_snapshot_transfer().is_none());
    assert!(!reopened.requires_reopen());

    let status = reopened.pending_snapshot_transfer_staging_status();
    match expected_residue {
        CleanupResidue::AbandonedBody => {
            assert!(!status.manifest_present);
            assert!(status.body_present);
            assert!(status.abandoned_body);
        }
        CleanupResidue::NoFiles => {
            assert!(!status.manifest_present);
            assert!(!status.body_present);
            assert!(!status.abandoned_body);
        }
    }
}
