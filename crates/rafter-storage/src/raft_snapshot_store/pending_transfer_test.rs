//! Pending-transfer staging, restart, promotion, and cleanup scenarios.

use std::fs;

use rafter_invariant_test::oracle_assert_eq;

use super::*;

use super::test_support::{
    assert_current_snapshot, pending_transfer, pending_transfer_for_payload, remove_test_dir,
    staged_chunk, staged_chunk_for_payload, test_store_dir, transfer_id_for, transfer_metadata,
};

#[test]
fn file_snapshot_store_reopens_pending_snapshot_transfer() {
    let directory = test_store_dir("pending-reopen");
    {
        let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");
        store
            .stage_snapshot_chunk(&staged_chunk(0, b"partial snapshot bytes", 64))
            .expect("chunk stages");
    }

    let reopened = FileRaftSnapshotStore::open(&directory).expect("store reopens");

    assert_eq!(
        reopened.current_pending_snapshot_transfer(),
        Some(pending_transfer(22, 64))
    );
    remove_test_dir(directory);
}

#[test]
fn file_snapshot_store_extends_pending_snapshot_body() {
    let directory = test_store_dir("pending-append");
    let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");

    store
        .stage_snapshot_chunk(&staged_chunk(0, b"partial", 64))
        .expect("first chunk stages");
    assert_eq!(
        fs::read(directory.join("pending.snapshot-transfer.body")).expect("body reads"),
        b"partial"
    );
    store
        .stage_snapshot_chunk(&staged_chunk(7, b" snapshot bytes", 64))
        .expect("second chunk appends");

    assert_eq!(
        fs::read(directory.join("pending.snapshot-transfer.body")).expect("body reads"),
        b"partial snapshot bytes"
    );
    assert_eq!(
        store.current_pending_snapshot_transfer(),
        Some(pending_transfer(22, 64))
    );
    assert_eq!(
        FileRaftSnapshotStore::open(&directory)
            .expect("store reopens")
            .current_pending_snapshot_transfer(),
        Some(pending_transfer(22, 64))
    );
    remove_test_dir(directory);
}

#[test]
fn file_snapshot_store_restarts_staging_on_offset_zero_chunk() {
    let directory = test_store_dir("pending-restart");
    let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");
    store
        .stage_snapshot_chunk(&staged_chunk(0, b"abandoned prefix", 64))
        .expect("first transfer stages");

    store
        .stage_snapshot_chunk(&staged_chunk(0, b"xy", 64))
        .expect("offset zero restarts staging");

    assert_eq!(
        fs::read(directory.join("pending.snapshot-transfer.body")).expect("body reads"),
        b"xy"
    );
    assert_eq!(
        store.current_pending_snapshot_transfer(),
        Some(pending_transfer(2, 64))
    );
    assert_eq!(
        FileRaftSnapshotStore::open(&directory)
            .expect("store reopens")
            .current_pending_snapshot_transfer(),
        Some(pending_transfer(2, 64))
    );
    remove_test_dir(directory);
}

#[test]
fn file_snapshot_store_rejects_continuation_chunk_without_staged_transfer() {
    let directory = test_store_dir("pending-no-transfer");
    let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");

    assert_eq!(
        store.stage_snapshot_chunk(&staged_chunk(7, b"tail", 64)),
        Err(RaftSnapshotStoreWriteError::StagedChunkWithoutTransfer {
            transfer_id: transfer_id_for(64),
            offset: 7,
        })
    );
    assert_eq!(store.current_pending_snapshot_transfer(), None);
    remove_test_dir(directory);
}

#[test]
fn file_snapshot_store_rejects_continuation_chunk_with_offset_gap() {
    let directory = test_store_dir("pending-offset-gap");
    let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");
    store
        .stage_snapshot_chunk(&staged_chunk(0, b"partial", 64))
        .expect("first chunk stages");

    assert_eq!(
        store.stage_snapshot_chunk(&staged_chunk(9, b"gap", 64)),
        Err(RaftSnapshotStoreWriteError::StagedChunkOffsetMismatch {
            expected_offset: 7,
            offset: 9,
        })
    );
    assert_eq!(
        store.current_pending_snapshot_transfer(),
        Some(pending_transfer(7, 64))
    );
    remove_test_dir(directory);
}

#[test]
fn file_snapshot_store_rejects_continuation_chunk_of_different_transfer() {
    let directory = test_store_dir("pending-wrong-transfer");
    let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");
    store
        .stage_snapshot_chunk(&staged_chunk(0, b"part", 64))
        .expect("first chunk stages");

    assert_eq!(
        store.stage_snapshot_chunk(&staged_chunk(4, b"tail", 32)),
        Err(RaftSnapshotStoreWriteError::StagedChunkTransferMismatch {
            staged_leader_id: rafter::NodeId(1),
            staged_transfer_id: transfer_id_for(64),
            leader_id: rafter::NodeId(1),
            transfer_id: transfer_id_for(32),
        })
    );
    remove_test_dir(directory);
}

#[test]
fn file_snapshot_store_promotes_completed_staged_transfer() {
    let directory = test_store_dir("pending-promote");
    let payload = b"opaque application snapshot";
    let expected = PersistedRaftSnapshot {
        metadata: transfer_metadata(),
        application_payload: payload.to_vec(),
    };
    let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");
    store
        .stage_snapshot_chunk(&staged_chunk_for_payload(0, &payload[..9], payload))
        .expect("first chunk stages");
    store
        .stage_snapshot_chunk(&staged_chunk_for_payload(9, &payload[9..], payload))
        .expect("final chunk stages");

    store
        .promote_staged_snapshot(&rafter::RaftSnapshot::from_payload(
            transfer_metadata(),
            payload,
        ))
        .expect("completed staged transfer promotes");

    assert_current_snapshot(&store, &expected);
    assert_eq!(store.current_pending_snapshot_transfer(), None);
    let reopened = FileRaftSnapshotStore::open(&directory).expect("store reopens");
    assert_current_snapshot(&reopened, &expected);
    assert_eq!(reopened.current_pending_snapshot_transfer(), None);
    remove_test_dir(directory);
}

#[test]
fn promotion_streams_staged_body_without_full_materialization() {
    let directory = test_store_dir("pending-promote-streamed");
    // Larger than one 256 KiB promotion stream chunk, staged across several
    // inbound chunks that straddle the stream chunk boundaries.
    let payload: Vec<u8> = (0_u32..160 * 1024).flat_map(u32::to_be_bytes).collect();
    let staged_chunk_len = 100 * 1024;
    let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");
    for (index, bytes) in payload.chunks(staged_chunk_len).enumerate() {
        let offset = (index * staged_chunk_len) as u64;
        store
            .stage_snapshot_chunk(&staged_chunk_for_payload(offset, bytes, &payload))
            .expect("staged chunk persists");
    }

    store
        .promote_staged_snapshot(&rafter::RaftSnapshot::from_payload(
            transfer_metadata(),
            &payload,
        ))
        .expect("completed staged transfer promotes");

    let reopened = FileRaftSnapshotStore::open(&directory).expect("store reopens");
    assert_current_snapshot(
        &reopened,
        &PersistedRaftSnapshot {
            metadata: transfer_metadata(),
            application_payload: payload,
        },
    );
    remove_test_dir(directory);
}

#[test]
fn file_snapshot_store_promotes_staged_transfer_resumed_after_reopen() {
    let directory = test_store_dir("pending-promote-after-reopen");
    let payload = b"abcdefghi";
    let total = payload.len() as u64;
    {
        let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");
        store
            .stage_snapshot_chunk(&staged_chunk_for_payload(0, &payload[..4], payload))
            .expect("first chunk stages");
    }

    let mut resumed = FileRaftSnapshotStore::open(&directory).expect("store reopens");
    oracle_assert_eq!(
        resumed.current_pending_snapshot_transfer(),
        Some(pending_transfer_for_payload(4, payload))
    );
    resumed
        .stage_snapshot_chunk(&staged_chunk_for_payload(4, &payload[4..], payload))
        .expect("final chunk appends after reopen");

    // A second reopen proves the appended manifest checksum covers the whole
    // staged body even though it was maintained incrementally.
    let mut store = FileRaftSnapshotStore::open(&directory).expect("store reopens again");
    oracle_assert_eq!(
        store.current_pending_snapshot_transfer(),
        Some(pending_transfer_for_payload(total, payload))
    );
    store
        .promote_staged_snapshot(&rafter::RaftSnapshot::from_payload(
            transfer_metadata(),
            payload,
        ))
        .expect("resumed staged transfer promotes");

    assert_current_snapshot(
        &store,
        &PersistedRaftSnapshot {
            metadata: transfer_metadata(),
            application_payload: payload.to_vec(),
        },
    );
    oracle_assert_eq!(store.current_pending_snapshot_transfer(), None);
    remove_test_dir(directory);
}

#[test]
fn file_snapshot_store_promotes_empty_staged_transfer() {
    let directory = test_store_dir("pending-promote-empty");
    let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");
    store
        .stage_snapshot_chunk(&staged_chunk(0, b"", 0))
        .expect("empty final chunk stages");

    store
        .promote_staged_snapshot(&rafter::RaftSnapshot::from_payload(
            transfer_metadata(),
            b"",
        ))
        .expect("empty staged transfer promotes");

    assert_current_snapshot(
        &store,
        &PersistedRaftSnapshot {
            metadata: transfer_metadata(),
            application_payload: Vec::new(),
        },
    );
    assert_eq!(store.current_pending_snapshot_transfer(), None);
    remove_test_dir(directory);
}

#[test]
fn file_snapshot_store_rejects_promotion_without_staged_transfer() {
    let directory = test_store_dir("pending-promote-missing");
    let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");

    assert_eq!(
        store.promote_staged_snapshot(&rafter::RaftSnapshot::new(transfer_metadata(), 9, 0)),
        Err(RaftSnapshotStoreWriteError::PromoteWithoutStagedTransfer {
            requested: transfer_id_for(9),
        })
    );
    remove_test_dir(directory);
}

#[test]
fn file_snapshot_store_rejects_promotion_of_incomplete_staged_transfer() {
    let directory = test_store_dir("pending-promote-incomplete");
    let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");
    store
        .stage_snapshot_chunk(&staged_chunk(0, b"partial snapshot bytes", 64))
        .expect("chunk stages");

    assert_eq!(
        store.promote_staged_snapshot(&rafter::RaftSnapshot::new(transfer_metadata(), 64, 0)),
        Err(
            RaftSnapshotStoreWriteError::PromoteIncompleteStagedTransfer {
                received_len: 22,
                total_payload_len: 64,
            }
        )
    );
    assert_eq!(
        store.current_pending_snapshot_transfer(),
        Some(pending_transfer(22, 64))
    );
    remove_test_dir(directory);
}

#[test]
fn file_snapshot_store_rejects_promotion_of_mismatched_transfer() {
    let directory = test_store_dir("pending-promote-mismatch");
    let mut store = FileRaftSnapshotStore::open(&directory).expect("store opens");
    store
        .stage_snapshot_chunk(&staged_chunk(0, b"abcd", 4))
        .expect("chunk stages");

    assert_eq!(
        store.promote_staged_snapshot(&rafter::RaftSnapshot::new(transfer_metadata(), 5, 0)),
        Err(RaftSnapshotStoreWriteError::PromoteTransferIdMismatch {
            staged: transfer_id_for(4),
            requested: transfer_id_for(5),
        })
    );
    remove_test_dir(directory);
}

#[test]
fn in_memory_snapshot_store_stages_and_promotes_snapshot_transfer() {
    let payload = b"opaque application snapshot";
    let mut store = InMemoryRaftSnapshotStore::new();

    store
        .stage_snapshot_chunk(&staged_chunk_for_payload(0, &payload[..9], payload))
        .expect("first chunk stages");
    assert_eq!(
        store.current_pending_snapshot_transfer(),
        Some(pending_transfer_for_payload(9, payload))
    );
    assert_eq!(
        store.stage_snapshot_chunk(&staged_chunk_for_payload(11, b"gap", payload)),
        Err(RaftSnapshotStoreWriteError::StagedChunkOffsetMismatch {
            expected_offset: 9,
            offset: 11,
        })
    );
    store
        .stage_snapshot_chunk(&staged_chunk_for_payload(9, &payload[9..], payload))
        .expect("final chunk stages");

    store
        .promote_staged_snapshot(&rafter::RaftSnapshot::from_payload(
            transfer_metadata(),
            payload,
        ))
        .expect("completed staged transfer promotes");

    assert_eq!(
        store.current(),
        Some(&PersistedRaftSnapshot {
            metadata: transfer_metadata(),
            application_payload: payload.to_vec(),
        })
    );
    assert_eq!(store.current_pending_snapshot_transfer(), None);
}

/// `None` means "nothing is staged", in both stores and across a reopen.
///
/// The owned read makes it possible to satisfy the signature by handing back a
/// copy of something. Nothing staged must still read as nothing staged: an
/// implementation that answered from a stale copy would leave the runtime's
/// open-time repair resuming a transfer the medium does not hold.
#[test]
fn an_empty_staging_area_reports_none() {
    let directory = test_store_dir("pending-empty-reports-none");
    let memory = InMemoryRaftSnapshotStore::new();
    let mut file = FileRaftSnapshotStore::open(&directory).expect("store opens");

    assert_eq!(memory.current_pending_snapshot_transfer(), None);
    assert_eq!(file.current_pending_snapshot_transfer(), None);

    // Clearing an already-empty staging area is not a way to acquire one.
    file.clear_pending_snapshot_transfer()
        .expect("clearing empty staging succeeds");
    assert_eq!(file.current_pending_snapshot_transfer(), None);
    assert_eq!(
        FileRaftSnapshotStore::open(&directory)
            .expect("store reopens")
            .current_pending_snapshot_transfer(),
        None
    );
    remove_test_dir(directory);
}

/// Promotion consumes the staged transfer, and no store may keep reporting it.
///
/// A promoted transfer that still reads back is the exact failure a cached copy
/// produces, and the runtime's open-time repair would promote it a second time
/// against a snapshot boundary that has already moved.
#[test]
fn a_promoted_transfer_reports_none() {
    let directory = test_store_dir("pending-promoted-reports-none");
    let payload = b"opaque application snapshot";
    let descriptor = rafter::RaftSnapshot::from_payload(transfer_metadata(), payload);
    let mut memory = InMemoryRaftSnapshotStore::new();
    let mut file = FileRaftSnapshotStore::open(&directory).expect("store opens");

    for (store, label) in [
        (&mut memory as &mut dyn RaftSnapshotStore, "in-memory"),
        (&mut file as &mut dyn RaftSnapshotStore, "file"),
    ] {
        store
            .stage_snapshot_chunk(&staged_chunk_for_payload(0, &payload[..9], payload))
            .expect("first chunk stages");
        store
            .stage_snapshot_chunk(&staged_chunk_for_payload(9, &payload[9..], payload))
            .expect("final chunk stages");
        assert_eq!(
            store.current_pending_snapshot_transfer(),
            Some(pending_transfer_for_payload(payload.len() as u64, payload)),
            "{label} store reports the completed staging before promotion"
        );

        store
            .promote_staged_snapshot(&descriptor)
            .expect("completed staged transfer promotes");

        assert_eq!(
            store.current_pending_snapshot_transfer(),
            None,
            "{label} store reports no staging after promotion"
        );
    }

    assert_eq!(
        FileRaftSnapshotStore::open(&directory)
            .expect("store reopens")
            .current_pending_snapshot_transfer(),
        None
    );
    remove_test_dir(directory);
}

#[test]
fn in_memory_snapshot_store_rejects_promotion_of_incomplete_staged_transfer() {
    let mut store = InMemoryRaftSnapshotStore::new();
    store
        .stage_snapshot_chunk(&staged_chunk(0, b"partial", 64))
        .expect("chunk stages");

    assert_eq!(
        store.promote_staged_snapshot(&rafter::RaftSnapshot::new(transfer_metadata(), 64, 0)),
        Err(
            RaftSnapshotStoreWriteError::PromoteIncompleteStagedTransfer {
                received_len: 7,
                total_payload_len: 64,
            }
        )
    );
}
