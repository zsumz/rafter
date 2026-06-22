use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion, LogIndex,
    NodeId, PendingSnapshotTransfer, RaftSnapshot, RaftSnapshotMetadata, SnapshotChunkRequest,
    SnapshotChunkSource, SnapshotGroupId, SnapshotTransferId, StagedSnapshotChunk, Term,
};

use super::RaftSnapshotStore;
use crate::{crc32, PersistedRaftSnapshot};

static TEST_DIR_ID: AtomicU64 = AtomicU64::new(0);

pub(super) fn snapshot(index: u64, term: u64, payload: &[u8]) -> PersistedRaftSnapshot {
    PersistedRaftSnapshot {
        metadata: RaftSnapshotMetadata::new(
            SnapshotGroupId::new("data-group-10").expect("valid group id"),
            NodeId(1),
            LogIndex(index),
            Term(term),
            Term(term),
            ApplicationSnapshotMetadata::new(
                ApplicationSnapshotKind::new("stream_data").expect("valid kind"),
                ApplicationSnapshotVersion::new(1).expect("valid version"),
            ),
        )
        .expect("snapshot metadata is valid"),
        application_payload: payload.to_vec(),
    }
}

pub(super) fn transfer_metadata() -> RaftSnapshotMetadata {
    snapshot(7, 6, b"").metadata
}

pub(super) fn transfer_id_for(total_payload_len: u64) -> SnapshotTransferId {
    transfer_id_for_checksum(total_payload_len, 0)
}

pub(super) fn transfer_id_for_payload(payload: &[u8]) -> SnapshotTransferId {
    transfer_id_for_checksum(payload.len() as u64, crc32(payload))
}

fn transfer_id_for_checksum(
    total_payload_len: u64,
    application_payload_crc32: u32,
) -> SnapshotTransferId {
    RaftSnapshot::new(
        transfer_metadata(),
        total_payload_len,
        application_payload_crc32,
    )
    .transfer_id()
}

/// One inbound chunk of the fixed test transfer identity; `done` is derived
/// from whether the chunk reaches the total payload length, mirroring the
/// kernel's chunk shape.
pub(super) fn staged_chunk(
    offset: u64,
    bytes: &[u8],
    total_payload_len: u64,
) -> StagedSnapshotChunk {
    StagedSnapshotChunk {
        leader_id: NodeId(1),
        transfer_id: transfer_id_for(total_payload_len),
        metadata: transfer_metadata(),
        total_payload_len,
        application_payload_crc32: 0,
        offset,
        bytes: bytes.to_vec(),
        done: offset + bytes.len() as u64 == total_payload_len,
    }
}

pub(super) fn staged_chunk_for_payload(
    offset: u64,
    bytes: &[u8],
    payload: &[u8],
) -> StagedSnapshotChunk {
    let application_payload_crc32 = crc32(payload);
    StagedSnapshotChunk {
        leader_id: NodeId(1),
        transfer_id: transfer_id_for_checksum(payload.len() as u64, application_payload_crc32),
        metadata: transfer_metadata(),
        total_payload_len: payload.len() as u64,
        application_payload_crc32,
        offset,
        bytes: bytes.to_vec(),
        done: offset + bytes.len() as u64 == payload.len() as u64,
    }
}

/// The store-visible staged transfer state after chunks of the fixed test
/// transfer identity covering `received_len` bytes have been staged.
pub(super) fn pending_transfer(
    received_len: u64,
    total_payload_len: u64,
) -> PendingSnapshotTransfer {
    PendingSnapshotTransfer {
        leader_id: NodeId(1),
        transfer_id: transfer_id_for(total_payload_len),
        metadata: transfer_metadata(),
        total_payload_len,
        application_payload_crc32: 0,
        received_len,
    }
}

pub(super) fn pending_transfer_for_payload(
    received_len: u64,
    payload: &[u8],
) -> PendingSnapshotTransfer {
    PendingSnapshotTransfer {
        leader_id: NodeId(1),
        transfer_id: transfer_id_for_payload(payload),
        metadata: transfer_metadata(),
        total_payload_len: payload.len() as u64,
        application_payload_crc32: crc32(payload),
        received_len,
    }
}

/// The kernel-facing descriptor of `snapshot`: metadata plus payload length.
pub(super) fn descriptor(snapshot: &PersistedRaftSnapshot) -> RaftSnapshot {
    RaftSnapshot::from_payload(snapshot.metadata.clone(), &snapshot.application_payload)
}

/// Streams the current snapshot's payload back out of `store` through its
/// chunk-source implementation in bounded requests — the only payload read
/// path the store trait offers.
pub(super) fn read_current_payload<S: RaftSnapshotStore + SnapshotChunkSource>(
    store: &S,
) -> Option<Vec<u8>> {
    const READ_CHUNK_BYTES: u64 = 256 * 1024;
    let descriptor = store.current_snapshot()?;
    let total = descriptor.application_payload_len;
    let mut payload = Vec::new();
    let mut offset = 0_u64;
    while offset < total {
        let len = u32::try_from((total - offset).min(READ_CHUNK_BYTES))
            .expect("read chunk length is bounded by the read chunk size");
        let bytes = store.snapshot_chunk(SnapshotChunkRequest {
            transfer_id: descriptor.transfer_id(),
            metadata: &descriptor.metadata,
            total_payload_len: total,
            application_payload_crc32: descriptor.application_payload_crc32,
            offset,
            len,
        })?;
        payload.extend_from_slice(&bytes);
        offset += u64::from(len);
    }
    Some(payload)
}

/// Asserts that `store`'s current snapshot is exactly `expected`: the
/// descriptor by value, and the payload streamed back through the store's
/// chunk source.
pub(super) fn assert_current_snapshot<S: RaftSnapshotStore + SnapshotChunkSource>(
    store: &S,
    expected: &PersistedRaftSnapshot,
) {
    assert_eq!(store.current_snapshot(), Some(descriptor(expected)));
    assert_eq!(
        read_current_payload(store).as_deref(),
        Some(expected.application_payload.as_slice())
    );
}

pub(super) fn test_store_dir(name: &str) -> PathBuf {
    let id = TEST_DIR_ID.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "rafter-snapshot-store-{name}-{}-{id}",
        std::process::id()
    ))
}

pub(super) fn remove_test_dir(path: PathBuf) {
    let _ = fs::remove_dir_all(path);
}
