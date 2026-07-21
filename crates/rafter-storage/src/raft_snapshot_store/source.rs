//! Snapshot payload sourcing and bounded chunk serving.
//!
//! This module pulls exact chunks from caller-provided sources and serves the
//! selected file-backed payload through positioned reads. It owns no publication
//! or staging policy.

use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
};

use rafter::{RaftSnapshot, SnapshotChunkRequest, SnapshotChunkSource, SnapshotTransferId};

use super::{state::snapshot_path, FileRaftSnapshotStore, RaftSnapshotStoreWriteError};

/// Pulls one payload chunk of `snapshot` from `source`, holding the source
/// to its contract: the chunk must exist and be exactly `len` bytes.
pub(super) fn source_chunk(
    source: &dyn SnapshotChunkSource,
    snapshot: &RaftSnapshot,
    transfer_id: SnapshotTransferId,
    offset: u64,
    len: u32,
) -> Result<Vec<u8>, RaftSnapshotStoreWriteError> {
    let bytes = source
        .snapshot_chunk(SnapshotChunkRequest {
            transfer_id,
            metadata: &snapshot.metadata,
            total_payload_len: snapshot.application_payload_len,
            application_payload_crc32: snapshot.application_payload_crc32,
            offset,
            len,
        })
        .ok_or(RaftSnapshotStoreWriteError::SourceChunkUnavailable {
            transfer_id,
            offset,
        })?;
    if bytes.len() == len as usize {
        Ok(bytes)
    } else {
        Err(RaftSnapshotStoreWriteError::SourceChunkUnavailable {
            transfer_id,
            offset,
        })
    }
}

pub(super) fn stream_chunk_len(remaining: u64, max_len: u32) -> u32 {
    let bounded = remaining.min(u64::from(max_len));
    u32::try_from(bounded).unwrap_or(max_len)
}

impl SnapshotChunkSource for FileRaftSnapshotStore {
    /// Serves payload chunks of the current snapshot by positioned reads of
    /// the envelope file — the payload is never resident. The request must
    /// match the complete selected descriptor: metadata, transfer id, length,
    /// and checksum. Any read failure yields `None`, which callers treat as a
    /// lost message.
    fn snapshot_chunk(&self, request: SnapshotChunkRequest<'_>) -> Option<Vec<u8>> {
        let current = self.current.as_ref()?;
        if current.descriptor.transfer_id() != request.transfer_id
            || &current.descriptor.metadata != request.metadata
            || current.descriptor.application_payload_len != request.total_payload_len
            || current.descriptor.application_payload_crc32 != request.application_payload_crc32
        {
            return None;
        }
        let end = request.offset.checked_add(u64::from(request.len))?;
        if end > current.descriptor.application_payload_len {
            return None;
        }
        let path = snapshot_path(&self.directory, &current.file_name);
        let mut file = File::open(path).ok()?;
        file.seek(SeekFrom::Start(current.payload_offset + request.offset))
            .ok()?;
        let mut bytes = vec![0u8; request.len as usize];
        file.read_exact(&mut bytes).ok()?;
        Some(bytes)
    }
}

/// Bytes pulled per chunk while streaming an envelope to disk.
pub(super) const SNAPSHOT_STREAM_CHUNK_BYTES: u32 = 256 * 1024;
