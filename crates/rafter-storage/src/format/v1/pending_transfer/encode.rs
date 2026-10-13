//! Encode the version-1 pending-transfer manifest and its nested metadata envelope.

use super::super::snapshot::{encode_raft_snapshot_metadata_envelope, EncodeRaftSnapshotError};
use super::{
    PendingTransferManifest, PENDING_SNAPSHOT_TRANSFER_MANIFEST_MAGIC,
    PENDING_SNAPSHOT_TRANSFER_MANIFEST_VERSION,
};
use crate::format::{finish_checksummed, Writer};

/// Encodes one canonical RFPT manifest.
///
/// # Errors
///
/// Returns [`EncodeRaftSnapshotError`] when the nested metadata-only RFSN
/// envelope cannot represent the snapshot metadata.
pub(crate) fn encode_pending_snapshot_transfer_manifest(
    manifest: &PendingTransferManifest,
) -> Result<Vec<u8>, EncodeRaftSnapshotError> {
    let metadata_envelope = encode_raft_snapshot_metadata_envelope(&manifest.metadata)?;

    let mut writer = Writer::new();
    writer.bytes(&PENDING_SNAPSHOT_TRANSFER_MANIFEST_MAGIC);
    writer.u8(PENDING_SNAPSHOT_TRANSFER_MANIFEST_VERSION);
    writer.u64(manifest.leader_id.0);
    writer.u64(manifest.transfer_id.0);
    writer.u64(manifest.total_payload_len);
    writer.u32(manifest.application_payload_crc32);
    writer.u64(manifest.received_payload_len);
    writer.u32(manifest.body_checksum);
    writer.u64(metadata_envelope.len() as u64);
    writer.bytes(&metadata_envelope);
    Ok(finish_checksummed(writer))
}
