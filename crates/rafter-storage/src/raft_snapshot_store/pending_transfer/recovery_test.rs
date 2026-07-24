//! Restart recovery scenarios for pending snapshot-transfer manifests and bodies.

use std::fs;

use rafter::{RaftSnapshot, SnapshotTransferId, StagedSnapshotChunk};

use super::super::{
    test_support::{remove_test_dir, test_store_dir, transfer_metadata},
    FileRaftSnapshotStore, OpenRaftSnapshotStoreError, RaftSnapshotStore,
};
use super::{
    codec::{decode_pending_snapshot_transfer_manifest, encode_pending_snapshot_transfer_manifest},
    error::DecodePendingSnapshotTransferError,
    manifest::PendingTransferManifest,
    paths::{pending_snapshot_transfer_body_path, pending_snapshot_transfer_path},
};
use crate::{
    crc32, encode_raft_snapshot,
    format::v1::pending_transfer::{
        MAX_PENDING_SNAPSHOT_METADATA_ENVELOPE_BYTES, PENDING_SNAPSHOT_TRANSFER_MANIFEST_MAGIC,
        PENDING_SNAPSHOT_TRANSFER_MANIFEST_VERSION,
    },
    PersistedRaftSnapshot,
};

fn manifest_for(
    total_payload_len: u64,
    application_payload_crc32: u32,
    received_payload: &[u8],
) -> PendingTransferManifest {
    let metadata = transfer_metadata();
    PendingTransferManifest {
        leader_id: rafter::NodeId(1),
        transfer_id: RaftSnapshot::new(
            metadata.clone(),
            total_payload_len,
            application_payload_crc32,
        )
        .transfer_id(),
        metadata,
        total_payload_len,
        application_payload_crc32,
        received_payload_len: received_payload.len() as u64,
        body_checksum: crc32(received_payload),
    }
}

fn write_manifest(directory: &std::path::Path, manifest: &PendingTransferManifest) {
    fs::create_dir_all(directory).expect("snapshot directory creates");
    let encoded = encode_pending_snapshot_transfer_manifest(manifest).expect("manifest encodes");
    fs::write(pending_snapshot_transfer_path(directory), encoded).expect("manifest writes");
}

fn encode_manifest_with_metadata_envelope(
    manifest: &PendingTransferManifest,
    declared_metadata_len: u64,
    metadata_envelope: &[u8],
) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&PENDING_SNAPSHOT_TRANSFER_MANIFEST_MAGIC);
    body.push(PENDING_SNAPSHOT_TRANSFER_MANIFEST_VERSION);
    body.extend_from_slice(&manifest.leader_id.0.to_be_bytes());
    body.extend_from_slice(&manifest.transfer_id.0.to_be_bytes());
    body.extend_from_slice(&manifest.total_payload_len.to_be_bytes());
    body.extend_from_slice(&manifest.application_payload_crc32.to_be_bytes());
    body.extend_from_slice(&manifest.received_payload_len.to_be_bytes());
    body.extend_from_slice(&manifest.body_checksum.to_be_bytes());
    body.extend_from_slice(&declared_metadata_len.to_be_bytes());
    body.extend_from_slice(metadata_envelope);
    let checksum = crc32(&body);
    body.extend_from_slice(&checksum.to_be_bytes());
    body
}

#[test]
fn recovery_rejects_transfer_id_not_derived_from_the_manifest_descriptor() {
    let directory = test_store_dir("pending-recovery-transfer-id");
    let received = b"partial";
    let mut manifest = manifest_for(64, 0x1234_abcd, received);
    let expected = manifest.transfer_id;
    manifest.transfer_id = SnapshotTransferId(expected.0 ^ 1);
    write_manifest(&directory, &manifest);
    fs::write(pending_snapshot_transfer_body_path(&directory), received)
        .expect("pending body writes");

    assert_eq!(
        FileRaftSnapshotStore::open(&directory)
            .map(|store| store.current_pending_snapshot_transfer()),
        Err(OpenRaftSnapshotStoreError::PendingTransfer(
            DecodePendingSnapshotTransferError::TransferIdMismatch {
                expected,
                actual: manifest.transfer_id,
            }
        ))
    );
    assert!(pending_snapshot_transfer_path(&directory).exists());
    assert!(pending_snapshot_transfer_body_path(&directory).exists());
    remove_test_dir(directory);
}

#[test]
fn recovery_rejects_received_length_past_total_before_looking_up_the_body() {
    let directory = test_store_dir("pending-recovery-length");
    let received = b"12345";
    let manifest = manifest_for(4, 0x1234_abcd, received);
    write_manifest(&directory, &manifest);

    assert_eq!(
        FileRaftSnapshotStore::open(&directory)
            .map(|store| store.current_pending_snapshot_transfer()),
        Err(OpenRaftSnapshotStoreError::PendingTransfer(
            DecodePendingSnapshotTransferError::ReceivedPayloadTooLong {
                received_bytes: 5,
                total_payload_len: 4,
            }
        ))
    );
    assert!(!pending_snapshot_transfer_body_path(&directory).exists());
    remove_test_dir(directory);
}

#[test]
fn recovery_discards_a_manifest_whose_optional_body_is_missing() {
    let directory = test_store_dir("pending-recovery-missing-body");
    let manifest = manifest_for(64, 0x1234_abcd, b"partial");
    write_manifest(&directory, &manifest);

    let store =
        FileRaftSnapshotStore::open(&directory).expect("missing optional body is discarded");

    assert_eq!(store.current_pending_snapshot_transfer(), None);
    assert!(!pending_snapshot_transfer_path(&directory).exists());
    assert!(!pending_snapshot_transfer_body_path(&directory).exists());
    remove_test_dir(directory);
}

#[test]
fn recovery_rebuilds_the_running_checksum_from_a_bounded_body_prefix() {
    let directory = test_store_dir("pending-recovery-streamed-prefix");
    let prefix: Vec<u8> = (0_u32..180 * 1024).flat_map(u32::to_be_bytes).collect();
    let continuation = b"the final recovered chunk";
    let mut complete_payload = prefix.clone();
    complete_payload.extend_from_slice(continuation);
    let manifest = manifest_for(
        complete_payload.len() as u64,
        crc32(&complete_payload),
        &prefix,
    );
    write_manifest(&directory, &manifest);
    let mut body_with_crash_suffix = prefix.clone();
    body_with_crash_suffix.extend_from_slice(b"unpublished crash suffix");
    fs::write(
        pending_snapshot_transfer_body_path(&directory),
        body_with_crash_suffix,
    )
    .expect("pending body with crash suffix writes");

    let mut store = FileRaftSnapshotStore::open(&directory).expect("staged prefix recovers");
    assert_eq!(
        store
            .current_pending_snapshot_transfer()
            .expect("pending transfer recovers")
            .received_len,
        prefix.len() as u64
    );

    store
        .stage_snapshot_chunk(&StagedSnapshotChunk {
            leader_id: manifest.leader_id,
            transfer_id: manifest.transfer_id,
            metadata: manifest.metadata.clone(),
            total_payload_len: manifest.total_payload_len,
            application_payload_crc32: manifest.application_payload_crc32,
            offset: prefix.len() as u64,
            bytes: continuation.to_vec(),
            done: true,
        })
        .expect("continuation stages from recovered checksum state");

    assert_eq!(
        fs::read(pending_snapshot_transfer_body_path(&directory)).expect("staged body reads"),
        complete_payload
    );
    assert_eq!(
        FileRaftSnapshotStore::open(&directory)
            .expect("completed staging reopens")
            .current_pending_snapshot_transfer()
            .expect("completed transfer remains staged")
            .received_len,
        manifest.total_payload_len
    );
    remove_test_dir(directory);
}

#[test]
fn codec_rejects_a_nested_snapshot_that_carries_application_payload() {
    let manifest = manifest_for(64, 0x1234_abcd, b"partial");
    let metadata_envelope = encode_raft_snapshot(&PersistedRaftSnapshot {
        metadata: manifest.metadata.clone(),
        application_payload: b"not metadata".to_vec(),
    })
    .expect("nested snapshot encodes");
    let envelope = encode_manifest_with_metadata_envelope(
        &manifest,
        metadata_envelope.len() as u64,
        &metadata_envelope,
    );

    assert_eq!(
        decode_pending_snapshot_transfer_manifest(&envelope),
        Err(
            DecodePendingSnapshotTransferError::SnapshotEnvelopePayloadNotEmpty {
                len: b"not metadata".len(),
            }
        )
    );
}

#[test]
fn codec_rejects_an_oversized_nested_metadata_envelope_before_slicing_it() {
    let manifest = manifest_for(64, 0x1234_abcd, b"partial");
    let declared = MAX_PENDING_SNAPSHOT_METADATA_ENVELOPE_BYTES + 1;
    let envelope = encode_manifest_with_metadata_envelope(&manifest, declared, &[]);

    assert_eq!(
        decode_pending_snapshot_transfer_manifest(&envelope),
        Err(DecodePendingSnapshotTransferError::SnapshotEnvelopeTooLarge { len: declared })
    );
}
