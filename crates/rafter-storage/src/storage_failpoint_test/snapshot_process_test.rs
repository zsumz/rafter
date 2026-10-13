//! Abrupt process loss at snapshot publication and transfer-cleanup boundaries.
//!
//! The kernel and page cache survive. This does not model physical power loss.

use std::{fs, path::Path};

use rafter::{RaftSnapshot, SnapshotChunkRequest, SnapshotChunkSource};

use crate::{FileRaftSnapshotStore, PersistedRaftSnapshot, RaftSnapshotStore};

use super::{
    arm_process_pause,
    support_test::{complete_staged_chunk, persisted_snapshot},
    DurabilityPoint,
};

mod process_test;

const TEST: &str =
    "storage_failpoint_test::snapshot_process_test::snapshot_publication_survives_process_loss";
const CHILD_ROOT: &str = "RAFTER_SNAPSHOT_PROCESS_CHILD";
const POINTS: [DurabilityPoint; 9] = [
    DurabilityPoint::SnapshotAfterTempSync,
    DurabilityPoint::SnapshotAfterFileRename,
    DurabilityPoint::SnapshotAfterFileDirectorySync,
    DurabilityPoint::SnapshotAfterManifestTempSync,
    DurabilityPoint::SnapshotAfterManifestRename,
    DurabilityPoint::SnapshotAfterManifestDirectorySync,
    DurabilityPoint::PendingClearAfterManifestRemoval,
    DurabilityPoint::PendingClearAfterBodyRemoval,
    DurabilityPoint::PendingClearAfterDirectorySync,
];

#[test]
#[ignore = "spawns and kills child processes at snapshot publication boundaries"]
fn snapshot_publication_survives_process_loss() {
    if let Some(root) = std::env::var_os(CHILD_ROOT) {
        run_child(Path::new(&root));
        return;
    }
    let workspace = process_test::Workspace::new();
    let mut completed = 0;
    for initial in [false, true] {
        for promotion in [false, true] {
            for (index, point) in POINTS.into_iter().enumerate() {
                run_case(workspace.path(), index, point, initial, promotion);
                completed += 1;
            }
        }
    }
    assert_eq!(completed, 36);
    fs::write(workspace.path().join("completed.txt"), "36 cases passed\n")
        .expect("completion receipt writes");
    println!("snapshot process matrix: {completed} verified SIGKILL/recovery cases");
}

fn run_case(root: &Path, index: usize, point: DurabilityPoint, initial: bool, promotion: bool) {
    let case = root.join(format!("initial-{initial}-promotion-{promotion}-{point:?}"));
    fs::create_dir(&case).expect("case directory creates exclusively");
    let old = persisted_snapshot(5, b"previously acknowledged snapshot");
    let payload: Vec<u8> = (0u8..251).cycle().take(512 * 1024 + 17).collect();
    let new = persisted_snapshot(9, &payload);
    let staged = if promotion {
        new.clone()
    } else {
        persisted_snapshot(7, b"obsolete received snapshot")
    };
    {
        let mut source = FileRaftSnapshotStore::open(case.join("source")).expect("source opens");
        source
            .write_snapshot(new.clone())
            .expect("source publishes");
        let mut target = FileRaftSnapshotStore::open(case.join("target")).expect("target opens");
        if initial {
            target
                .write_snapshot(old.clone())
                .expect("old snapshot acknowledges");
        }
        target
            .stage_snapshot_chunk(&complete_staged_chunk(&staged))
            .expect("incoming bytes stage");
    }
    fs::write(
        case.join("operation.txt"),
        format!("{index}\n{promotion}\n"),
    )
    .expect("child operation records");
    let pid = process_test::kill_at_boundary(&case, TEST, CHILD_ROOT, point);
    process_test::preserve_inputs(&case.join("target"), &case.join("pre-recovery"));
    let mut recovered = FileRaftSnapshotStore::open(case.join("target"))
        .expect("strict recovery succeeds after verified process loss");
    assert!(!recovered.requires_reopen());
    let committed = index >= 4;
    if committed {
        assert_snapshot(&recovered, &new);
    } else if initial {
        assert_snapshot(&recovered, &old);
    } else {
        assert!(recovered.current_snapshot().is_none());
    }
    let pending = recovered.current_pending_snapshot_transfer();
    if index < 6 {
        let pending = pending.expect("publication did not remove durable staging yet");
        let expected = complete_staged_chunk(&staged);
        assert_eq!(pending.transfer_id, expected.transfer_id);
        assert_eq!(pending.received_len, expected.total_payload_len);
    } else {
        assert!(pending.is_none());
    }
    let status = recovered.pending_snapshot_transfer_staging_status();
    assert_eq!(status.manifest_present, index < 6);
    assert_eq!(status.body_present, index < 7);
    assert_eq!(status.abandoned_body, index == 6);

    let next = persisted_snapshot(12, b"publication after strict recovery");
    recovered
        .write_snapshot(next.clone())
        .expect("recovered store remains writable");
    drop(recovered);
    let reopened = FileRaftSnapshotStore::open(case.join("target")).expect("next snapshot reopens");
    assert_snapshot(&reopened, &next);
    assert!(reopened.current_pending_snapshot_transfer().is_none());
    let receipt =
        format!("point={point:?}\npid={pid}\nsignal=9\nnew_current={committed}\nnext_index=12\n");
    fs::write(case.join("outcome.txt"), receipt).expect("case receipt writes");
    println!(
        "snapshot process case: initial={initial} promotion={promotion} point={point:?} passed"
    );
}

fn run_child(root: &Path) {
    let operation = fs::read_to_string(root.join("operation.txt")).expect("child operation reads");
    let mut fields = operation.lines();
    let index: usize = fields
        .next()
        .expect("point index")
        .parse()
        .expect("numeric point");
    let promotion: bool = fields
        .next()
        .expect("operation kind")
        .parse()
        .expect("boolean kind");
    assert!(fields.next().is_none());
    let point = POINTS[index];
    let source = FileRaftSnapshotStore::open(root.join("source")).expect("child source opens");
    let descriptor = source.current_snapshot().expect("source snapshot exists");
    let mut target = FileRaftSnapshotStore::open(root.join("target")).expect("child target opens");
    let _guard = arm_process_pause(point, root.join("boundary.txt"));
    if promotion {
        target
            .promote_staged_snapshot(&descriptor)
            .expect("promotion reaches the armed boundary");
    } else {
        target
            .write_snapshot_from_source(&descriptor, &source)
            .expect("streamed publication reaches the armed boundary");
    }
    panic!("publication returned without reaching the armed process boundary");
}

fn assert_snapshot(store: &FileRaftSnapshotStore, expected: &PersistedRaftSnapshot) {
    let descriptor =
        RaftSnapshot::from_payload(expected.metadata.clone(), &expected.application_payload);
    assert_eq!(store.current_snapshot(), Some(descriptor.clone()));
    let mut offset = 0;
    while offset < expected.application_payload.len() {
        let end = (offset + 16 * 1024).min(expected.application_payload.len());
        let bytes = store
            .snapshot_chunk(SnapshotChunkRequest {
                transfer_id: descriptor.transfer_id(),
                metadata: &descriptor.metadata,
                total_payload_len: descriptor.application_payload_len,
                application_payload_crc32: descriptor.application_payload_crc32,
                offset: offset as u64,
                len: u32::try_from(end - offset).expect("bounded chunk length fits u32"),
            })
            .expect("recovered descriptor serves a complete chunk");
        assert_eq!(bytes, expected.application_payload[offset..end]);
        offset = end;
    }
}
