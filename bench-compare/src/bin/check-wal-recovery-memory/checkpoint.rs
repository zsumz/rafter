//! Real checkpoint publication followed by truncated declared entry bodies.
use super::{fs, io, LogIndex, Measurement, Ordering, Scratch, Term, LARGEST, REQUESTED};
use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion, NodeId,
    RaftSnapshotMetadata, SnapshotGroupId,
};
use rafter_storage::{
    durable_batch::{RaftPersistenceBatch, WalRaftNodeStores},
    BorrowedPersistedRaftLogEntry, PersistedRaftLogEntry, PersistedRaftSnapshot, RaftHardState,
    RaftHardStateStore, RaftLogSegment, RaftSnapshotStore,
};

pub(super) fn check() {
    let directory = Scratch::new(0, 0);
    publish_checkpoint(&directory);
    let path = directory
        .0
        .join("raft-wal-checkpoint-00000000000000000001.rfwc");
    let original = fs::read(&path).expect("published checkpoint");
    assert_eq!(&original[..8], b"RFWC\0\0\0\x01");
    assert_eq!(&original[108..116], &1_u64.to_be_bytes());
    let manifest_path = directory.0.join("raft-wal-current");
    let manifest = fs::read(&manifest_path).expect("selected generation");
    let segment_path = directory
        .0
        .join("raft-wal-segment-00000000000000000001.rfwb");
    let segment = fs::read(&segment_path).expect("selected segment");

    for declared in [8 * 1024 * 1024_u32, 64 * 1024 * 1024] {
        for available in [0, 17, 64 * 1024] {
            let mut truncated = original[..120].to_vec();
            truncated[116..120].copy_from_slice(&declared.to_be_bytes());
            truncated.resize(120 + available, 0);
            fs::write(&path, &truncated).expect("truncate declared entry body");
            let measurement = Measurement::start();
            let result = WalRaftNodeStores::open(&directory.0);
            drop(measurement);
            let largest = LARGEST.load(Ordering::Relaxed);
            let requested = REQUESTED.load(Ordering::Relaxed);
            let error = result.expect_err("selected checkpoint must not be repaired");
            assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
            assert_eq!(error.to_string(), "failed to fill whole buffer");
            assert_eq!(fs::read(&path).expect("unchanged checkpoint"), truncated);
            assert_eq!(
                fs::read(&manifest_path).expect("unchanged manifest"),
                manifest
            );
            assert_eq!(fs::read(&segment_path).expect("unchanged segment"), segment);
            // Include fixed read buffers, snapshot verification and paths. The
            // bound follows supplied bytes, never the forged 8/64 MiB length.
            let largest_limit = 1024 * 1024 + 2 * available;
            let requested_limit = 2 * 1024 * 1024 + 4 * available;
            println!(
                "checkpoint declared={declared}, available={available}, \
                 largest_request={largest}/{largest_limit}, \
                 total_requested={requested}/{requested_limit}"
            );
            assert!(
                largest <= largest_limit,
                "checkpoint length amplified allocation"
            );
            assert!(
                requested <= requested_limit,
                "checkpoint length amplified cumulative allocations"
            );
        }
    }
    fs::write(&path, original).expect("restore exact published checkpoint");
    verify_reopen(&directory);
}

fn publish_checkpoint(directory: &Scratch) {
    let (hard, mut log, mut snapshots) = WalRaftNodeStores::open(&directory.0)
        .expect("initialize independent WAL")
        .into_parts();
    let entries = [
        PersistedRaftLogEntry::noop(LogIndex(1), Term(1)),
        PersistedRaftLogEntry::application(LogIndex(2), Term(1), b"retained".to_vec()),
    ];
    let borrowed: Vec<_> = entries
        .iter()
        .map(|entry| BorrowedPersistedRaftLogEntry::new(entry.index, entry.term, &entry.kind))
        .collect();
    log.persist_batch(
        &hard.persistence_domain().expect("WAL persistence domain"),
        RaftPersistenceBatch {
            truncate_from: None,
            entries: &borrowed,
            hard_state: Some(RaftHardState {
                current_term: Term(1),
                commit_index: LogIndex(2),
                ..RaftHardState::default()
            }),
        },
    )
    .expect("persist entries")
    .expect("WAL supports atomic batches");
    snapshots
        .write_snapshot(PersistedRaftSnapshot {
            metadata: RaftSnapshotMetadata::new(
                SnapshotGroupId::new("checkpoint-memory").expect("group"),
                NodeId(1),
                LogIndex(1),
                Term(1),
                Term(1),
                ApplicationSnapshotMetadata::new(
                    ApplicationSnapshotKind::new("allocation-gate").expect("kind"),
                    ApplicationSnapshotVersion::new(1).expect("version"),
                ),
            )
            .expect("metadata"),
            application_payload: b"state-through-1".to_vec(),
        })
        .expect("publish snapshot");
    log.compact_prefix_through(LogIndex(1))
        .expect("publish checkpoint");
    drop((hard, log, snapshots));
    verify_reopen(directory);
}

fn verify_reopen(directory: &Scratch) {
    let (hard, log, snapshots) = WalRaftNodeStores::open(&directory.0)
        .expect("original checkpoint reopens")
        .into_parts();
    assert_eq!(hard.current().commit_index, LogIndex(2));
    assert_eq!(log.compacted_through(), LogIndex(1));
    assert_eq!(
        log.replay_entries(),
        vec![PersistedRaftLogEntry::application(
            LogIndex(2),
            Term(1),
            b"retained".to_vec()
        )]
    );
    assert_eq!(
        snapshots
            .current_snapshot()
            .expect("snapshot")
            .metadata
            .last_included_index,
        LogIndex(1)
    );
}
