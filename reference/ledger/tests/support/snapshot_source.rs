//! Test-owned private staging, synced before exposing an immutable chunk source.

use std::{
    error::Error,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom},
    sync::Mutex,
};

use rafter::{RaftSnapshot, SnapshotChunkRequest, SnapshotChunkSource};

use crate::scratch::ScratchDir;

pub struct FileSource {
    snapshot: RaftSnapshot,
    file: Mutex<File>,
    // Keep staging alive through compaction, then close the file before cleanup.
    _scratch: ScratchDir,
}

impl FileSource {
    pub fn build(
        encode: impl FnOnce(&mut File) -> Result<RaftSnapshot, Box<dyn Error + Send + Sync>>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let scratch = ScratchDir::new("ledger-snapshot-source");
        let path = scratch.path().join("payload");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        let snapshot = encode(&mut file)?;
        file.sync_all()?;
        drop(file);
        let file = File::open(path)?;
        assert_eq!(file.metadata()?.len(), snapshot.application_payload_len);
        Ok(Self {
            snapshot,
            file: Mutex::new(file),
            _scratch: scratch,
        })
    }

    pub fn snapshot(&self) -> &RaftSnapshot {
        &self.snapshot
    }
}

impl SnapshotChunkSource for FileSource {
    fn snapshot_chunk(&self, request: SnapshotChunkRequest<'_>) -> Option<Vec<u8>> {
        if request.transfer_id != self.snapshot.transfer_id()
            || request.metadata != &self.snapshot.metadata
            || request.total_payload_len != self.snapshot.application_payload_len
            || request.application_payload_crc32 != self.snapshot.application_payload_crc32
            || request.offset.checked_add(u64::from(request.len))? > request.total_payload_len
            || request.len > 256 * 1024
        {
            return None;
        }
        let mut file = self.file.lock().ok()?;
        file.seek(SeekFrom::Start(request.offset)).ok()?;
        let mut bytes = vec![0; usize::try_from(request.len).ok()?];
        file.read_exact(&mut bytes).ok()?;
        Some(bytes)
    }
}
