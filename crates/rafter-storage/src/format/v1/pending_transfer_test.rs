//! Scenarios for version-1 pending snapshot-transfer manifest behavior.

use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion, LogIndex,
    NodeId, RaftSnapshot, RaftSnapshotMetadata, SnapshotGroupId, Term,
};

use super::pending_transfer::{
    decode_pending_snapshot_transfer_manifest, encode_pending_snapshot_transfer_manifest,
    DecodePendingSnapshotTransferError, PendingTransferManifest,
    PENDING_SNAPSHOT_TRANSFER_MANIFEST_MAGIC, PENDING_SNAPSHOT_TRANSFER_MANIFEST_VERSION,
};

fn metadata() -> RaftSnapshotMetadata {
    RaftSnapshotMetadata::new(
        SnapshotGroupId::new("data-group-10").expect("valid group id"),
        NodeId(1),
        LogIndex(7),
        Term(6),
        Term(6),
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("stream_data").expect("valid kind"),
            ApplicationSnapshotVersion::new(1).expect("valid version"),
        ),
    )
    .expect("valid snapshot metadata")
}

fn manifest() -> PendingTransferManifest {
    let metadata = metadata();
    let total_payload_len = 64;
    let application_payload_crc32 = 0;
    PendingTransferManifest {
        leader_id: NodeId(1),
        transfer_id: RaftSnapshot::new(
            metadata.clone(),
            total_payload_len,
            application_payload_crc32,
        )
        .transfer_id(),
        metadata,
        total_payload_len,
        application_payload_crc32,
        received_payload_len: 12,
        body_checksum: 0,
    }
}

#[test]
fn pending_transfer_manifest_round_trips_through_rfpt() {
    let manifest = manifest();
    let encoded = encode_pending_snapshot_transfer_manifest(&manifest).expect("manifest encodes");

    assert_eq!(
        decode_pending_snapshot_transfer_manifest(&encoded),
        Ok(manifest)
    );
}

#[test]
fn pending_transfer_manifest_rejects_an_unsupported_version() {
    let mut encoded =
        encode_pending_snapshot_transfer_manifest(&manifest()).expect("manifest encodes");
    encoded[PENDING_SNAPSHOT_TRANSFER_MANIFEST_MAGIC.len()] =
        PENDING_SNAPSHOT_TRANSFER_MANIFEST_VERSION + 1;
    let checksum_start = encoded.len() - std::mem::size_of::<u32>();
    let checksum = crate::crc32(&encoded[..checksum_start]);
    encoded[checksum_start..].copy_from_slice(&checksum.to_be_bytes());

    assert_eq!(
        decode_pending_snapshot_transfer_manifest(&encoded),
        Err(DecodePendingSnapshotTransferError::UnsupportedVersion(
            PENDING_SNAPSHOT_TRANSFER_MANIFEST_VERSION + 1,
        ))
    );
}
