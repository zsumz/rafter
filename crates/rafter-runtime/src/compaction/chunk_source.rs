//! Chunk source that answers only for the descriptor it was built from.
//!
//! A streamed local compaction fills membership into the descriptor it
//! persists, so the bytes the application streams must still be requested
//! under the original descriptor's identity. This adapter re-stamps every
//! request and refuses any whose length or checksum does not match.

use rafter::{RaftSnapshot, SnapshotChunkRequest, SnapshotChunkSource};

pub(super) struct OriginalSnapshotChunkSource<'a> {
    pub(super) source: &'a dyn SnapshotChunkSource,
    pub(super) snapshot: &'a RaftSnapshot,
}

impl SnapshotChunkSource for OriginalSnapshotChunkSource<'_> {
    fn snapshot_chunk(&self, request: SnapshotChunkRequest<'_>) -> Option<Vec<u8>> {
        if request.total_payload_len != self.snapshot.application_payload_len
            || request.application_payload_crc32 != self.snapshot.application_payload_crc32
        {
            return None;
        }
        self.source.snapshot_chunk(SnapshotChunkRequest {
            transfer_id: self.snapshot.transfer_id(),
            metadata: &self.snapshot.metadata,
            total_payload_len: self.snapshot.application_payload_len,
            application_payload_crc32: self.snapshot.application_payload_crc32,
            offset: request.offset,
            len: request.len,
        })
    }
}
