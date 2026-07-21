//! Snapshot chunk-shape, identity, descriptor, and source-boundary scenarios.

use rafter::{
    NodeId, PendingSnapshotTransfer, RaftSnapshot, SnapshotChunkRequest, SnapshotChunkSource,
    SnapshotTransferId, StagedSnapshotChunk,
};

use super::test_support::{
    descriptor, remove_test_dir, snapshot, staged_chunk, staged_chunk_for_payload, test_store_dir,
    transfer_metadata,
};
use super::{validation::validate_staged_promotion, *};

fn assert_stage_rejected_by_both(
    name: &str,
    chunk: &StagedSnapshotChunk,
    expected: RaftSnapshotStoreWriteError,
) {
    let mut memory = InMemoryRaftSnapshotStore::new();
    assert_eq!(memory.stage_snapshot_chunk(chunk), Err(expected.clone()));
    assert_eq!(memory.current_pending_snapshot_transfer(), None);

    let directory = test_store_dir(name);
    let mut file = FileRaftSnapshotStore::open(&directory).expect("file store opens");
    assert_eq!(file.stage_snapshot_chunk(chunk), Err(expected));
    assert!(!file.requires_reopen());
    assert_eq!(file.current_pending_snapshot_transfer(), None);
    assert!(!directory.join("pending.snapshot-transfer").exists());
    assert!(!directory.join("pending.snapshot-transfer.body").exists());

    let valid = staged_chunk(0, b"", 0);
    memory
        .stage_snapshot_chunk(&valid)
        .expect("memory store remains writable after validation failure");
    file.stage_snapshot_chunk(&valid)
        .expect("file store remains writable after validation failure");
    assert_eq!(
        memory
            .current_pending_snapshot_transfer()
            .expect("memory transfer stages")
            .received_len,
        0
    );
    assert_eq!(
        file.current_pending_snapshot_transfer()
            .expect("file transfer stages")
            .received_len,
        0
    );
    remove_test_dir(directory);
}

#[test]
fn staging_rejects_range_overflow_before_mutating_either_store() {
    let mut chunk = staged_chunk(0, b"x", u64::MAX);
    chunk.offset = u64::MAX;
    chunk.done = false;

    assert_stage_rejected_by_both(
        "validation-range-overflow",
        &chunk,
        RaftSnapshotStoreWriteError::StagedChunkRangeOverflow {
            offset: u64::MAX,
            len: 1,
        },
    );
}

#[test]
fn staging_rejects_chunks_past_the_advertised_payload() {
    let chunk = staged_chunk(0, b"abc", 2);

    assert_stage_rejected_by_both(
        "validation-past-end",
        &chunk,
        RaftSnapshotStoreWriteError::StagedChunkPastEnd {
            offset: 0,
            len: 3,
            total_payload_len: 2,
        },
    );
}

#[test]
fn staging_rejects_empty_non_final_chunks() {
    let chunk = staged_chunk(0, b"", 4);

    assert_stage_rejected_by_both(
        "validation-empty-before-end",
        &chunk,
        RaftSnapshotStoreWriteError::StagedChunkEmptyBeforeEnd {
            offset: 0,
            total_payload_len: 4,
        },
    );
}

#[test]
fn staging_rejects_both_finality_disagreements() {
    let mut complete_but_not_done = staged_chunk(0, b"abc", 3);
    complete_but_not_done.done = false;
    assert_stage_rejected_by_both(
        "validation-complete-not-done",
        &complete_but_not_done,
        RaftSnapshotStoreWriteError::StagedChunkDoneMismatch {
            done: false,
            end_offset: 3,
            total_payload_len: 3,
        },
    );

    let mut early_but_done = staged_chunk(0, b"a", 3);
    early_but_done.done = true;
    assert_stage_rejected_by_both(
        "validation-early-done",
        &early_but_done,
        RaftSnapshotStoreWriteError::StagedChunkDoneMismatch {
            done: true,
            end_offset: 1,
            total_payload_len: 3,
        },
    );
}

#[test]
fn staging_rejects_a_transfer_id_not_derived_from_the_descriptor() {
    let mut chunk = staged_chunk(0, b"", 0);
    let expected = chunk.transfer_id;
    chunk.transfer_id = SnapshotTransferId(expected.0 ^ 1);
    let actual = chunk.transfer_id;

    assert_stage_rejected_by_both(
        "validation-transfer-id",
        &chunk,
        RaftSnapshotStoreWriteError::StagedChunkTransferIdMismatch { expected, actual },
    );
}

#[test]
fn promotion_compares_the_full_descriptor_after_transfer_id_match() {
    let requested = RaftSnapshot::new(transfer_metadata(), 4, 0x1234_5678);
    let requested_id = requested.transfer_id();
    let staged_descriptors = [
        RaftSnapshot::new(snapshot(8, 7, b"").metadata, 4, 0x1234_5678),
        RaftSnapshot::new(requested.metadata.clone(), 5, 0x1234_5678),
        RaftSnapshot::new(requested.metadata.clone(), 4, 0x8765_4321),
    ];

    for staged in staged_descriptors {
        let current = PendingSnapshotTransfer {
            leader_id: NodeId(1),
            // Deliberately model a transfer-id collision or corrupt recovered
            // state: the routing id matches while the full descriptor does not.
            transfer_id: requested_id,
            metadata: staged.metadata.clone(),
            total_payload_len: staged.application_payload_len,
            application_payload_crc32: staged.application_payload_crc32,
            received_len: staged.application_payload_len,
        };

        assert_eq!(
            validate_staged_promotion(&requested, Some(&current)),
            Err(
                RaftSnapshotStoreWriteError::PromoteSnapshotDescriptorMismatch {
                    staged: Box::new(staged),
                    requested: Box::new(requested.clone()),
                }
            )
        );
    }
}

#[test]
fn snapshot_sources_require_request_metadata_to_match_the_current_descriptor() {
    let current = snapshot(3, 2, b"application snapshot");
    let current_descriptor = descriptor(&current);
    let other_metadata = snapshot(4, 3, b"").metadata;
    let request = || SnapshotChunkRequest {
        transfer_id: current_descriptor.transfer_id(),
        metadata: &other_metadata,
        total_payload_len: current_descriptor.application_payload_len,
        application_payload_crc32: current_descriptor.application_payload_crc32,
        offset: 0,
        len: 4,
    };

    let memory = InMemoryRaftSnapshotStore::with_snapshot(current.clone());
    assert_eq!(memory.snapshot_chunk(request()), None);

    let directory = test_store_dir("validation-source-metadata");
    let mut file = FileRaftSnapshotStore::open(&directory).expect("file store opens");
    file.write_snapshot(current).expect("snapshot writes");
    assert_eq!(file.snapshot_chunk(request()), None);
    remove_test_dir(directory);
}

#[test]
fn checked_received_length_is_shared_by_memory_and_file_staging() {
    let payload = b"abcdef";
    let first = staged_chunk_for_payload(0, &payload[..2], payload);
    let second = staged_chunk_for_payload(2, &payload[2..], payload);

    let mut memory = InMemoryRaftSnapshotStore::new();
    memory
        .stage_snapshot_chunk(&first)
        .expect("first chunk stages");
    memory
        .stage_snapshot_chunk(&second)
        .expect("second chunk stages");
    assert_eq!(
        memory
            .current_pending_snapshot_transfer()
            .expect("memory transfer is staged")
            .received_len,
        payload.len() as u64
    );

    let directory = test_store_dir("validation-checked-length");
    let mut file = FileRaftSnapshotStore::open(&directory).expect("file store opens");
    file.stage_snapshot_chunk(&first)
        .expect("first chunk stages");
    file.stage_snapshot_chunk(&second)
        .expect("second chunk stages");
    assert_eq!(
        file.current_pending_snapshot_transfer()
            .expect("file transfer is staged")
            .received_len,
        payload.len() as u64
    );
    remove_test_dir(directory);
}
