//! Prepare-first publication of one pending snapshot-transfer chunk.
//!
//! Manifest encoding is completed before the body file is mutated. The body is
//! then made durable before the manifest publishes the new authoritative staged
//! length and checksum.

use std::path::Path;

use rafter::StagedSnapshotChunk;

use crate::checksum::RunningCrc32;

use super::{
    super::RaftSnapshotStoreWriteError, body::write_pending_snapshot_body_chunk,
    codec::encode_pending_snapshot_transfer_manifest, filesystem::write_temp_and_rename,
    manifest::PendingTransferManifest, paths::pending_snapshot_transfer_path,
};

/// Stages one validated chunk durably: prepares the next manifest, writes the
/// chunk bytes into the body file, then publishes the manifest with the
/// extended staged length and body checksum.
///
/// `body_crc_before_chunk` is the running checksum of the staged body before
/// this chunk — a fresh state for a chunk at offset zero. Returns the running
/// checksum after the chunk, which the caller carries to the next append so
/// the manifest checksum never requires re-reading the body file.
///
/// `received_len` is the range-checked end offset returned by the shared
/// staging validator.
pub(in crate::raft_snapshot_store) fn stage_pending_snapshot_chunk(
    directory: &Path,
    temp_manifest_path: &Path,
    chunk: &StagedSnapshotChunk,
    received_len: u64,
    body_crc_before_chunk: RunningCrc32,
) -> Result<RunningCrc32, RaftSnapshotStoreWriteError> {
    let mut body_crc = body_crc_before_chunk;
    body_crc.update(&chunk.bytes);
    let manifest = PendingTransferManifest {
        leader_id: chunk.leader_id,
        transfer_id: chunk.transfer_id,
        metadata: chunk.metadata.clone(),
        total_payload_len: chunk.total_payload_len,
        application_payload_crc32: chunk.application_payload_crc32,
        received_payload_len: received_len,
        body_checksum: body_crc.value(),
    };
    let encoded = encode_pending_snapshot_transfer_manifest(&manifest)
        .map_err(RaftSnapshotStoreWriteError::EncodeSnapshot)?;

    write_pending_snapshot_body_chunk(directory, chunk)?;
    write_temp_and_rename(
        temp_manifest_path,
        &pending_snapshot_transfer_path(directory),
        &encoded,
        "open pending snapshot transfer manifest temp file",
        "write pending snapshot transfer manifest temp file",
        "replace pending snapshot transfer manifest",
        "sync pending snapshot transfer manifest directory",
    )?;
    Ok(body_crc)
}
