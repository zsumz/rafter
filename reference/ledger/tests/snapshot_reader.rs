//! An external opaque-blob application creates and installs through public streams.
//!
//! Real snapshot files feed a private staging file; only a verified receipt
//! permits publication of bytes and applied floor together. This tests normal
//! reopen and failed attempts, not power-loss guarantees of the host filesystem.

#[path = "support/scratch.rs"]
mod scratch;

use std::{
    cell::Cell,
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::Path,
    sync::Mutex,
};

use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion, LogIndex,
    NodeId, RaftSnapshot, RaftSnapshotMetadata, SnapshotChunkRequest, SnapshotChunkSource,
    SnapshotGroupId, Term,
};
use rafter_runtime::snapshot_io::{
    SnapshotCancellation, SnapshotPayloadReader, SnapshotPayloadWriter, SnapshotReadOptions,
    SnapshotWriteOptions,
};
use rafter_storage::{FileRaftSnapshotStore, RaftSnapshotStore};
use scratch::ScratchDir;

// Independently calculated IEEE CRC32 of exactly this constant byte sequence.
const LENGTH: u64 = 2 * 1024 * 1024 + 17;
const CRC32: u32 = 0xed60_5685;
const CHUNK: u32 = 4096;

struct StagedFile {
    snapshot: RaftSnapshot,
    file: Mutex<File>,
}
impl SnapshotChunkSource for StagedFile {
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
        let mut bytes = vec![0; request.len as usize];
        file.read_exact(&mut bytes).ok()?;
        Some(bytes)
    }
}

fn source(path: &Path) -> (FileRaftSnapshotStore, RaftSnapshot) {
    let metadata = RaftSnapshotMetadata::new(
        SnapshotGroupId::new("opaque-reader").unwrap(),
        NodeId(1),
        LogIndex(42),
        Term(3),
        Term(3),
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("opaque-blob").unwrap(),
            ApplicationSnapshotVersion::new(1).unwrap(),
        ),
    )
    .unwrap();
    let staging_path = path.with_extension("staging");
    let staging = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&staging_path)
        .unwrap();
    let mut writer = SnapshotPayloadWriter::new(
        staging,
        metadata,
        LogIndex(42),
        SnapshotWriteOptions::new(LENGTH).with_chunk_bytes(CHUNK),
        SnapshotCancellation::new(),
    )
    .unwrap();
    let encoded = [0x5a; 113];
    let mut remaining = LENGTH;
    while remaining != 0 {
        let count = usize::try_from(remaining.min(encoded.len() as u64)).unwrap();
        writer.write_all(&encoded[..count]).unwrap();
        remaining -= count as u64;
    }
    let (staging, snapshot) = writer.finish().unwrap();
    assert_eq!(snapshot.application_payload_len, LENGTH);
    assert_eq!(snapshot.application_payload_crc32, CRC32);
    staging.sync_all().unwrap(); // Explicit caller obligation, not writer.finish.
    drop(staging);
    let source = StagedFile {
        snapshot: snapshot.clone(),
        file: Mutex::new(File::open(&staging_path).unwrap()),
    };
    let mut store = FileRaftSnapshotStore::open(path).unwrap();
    store
        .write_snapshot_from_source(&snapshot, &source)
        .unwrap();
    drop(source);
    fs::remove_file(staging_path).unwrap();
    drop(store);
    let reopened = FileRaftSnapshotStore::open(path).unwrap();
    assert_eq!(reopened.current_snapshot(), Some(snapshot.clone()));
    (reopened, snapshot)
}

struct Observed<'a> {
    source: &'a dyn SnapshotChunkSource,
    calls: Cell<usize>,
    cancel: Option<SnapshotCancellation>,
    corrupt: bool,
}
impl SnapshotChunkSource for Observed<'_> {
    fn snapshot_chunk(&self, request: SnapshotChunkRequest<'_>) -> Option<Vec<u8>> {
        assert!(request.len <= CHUNK);
        self.calls.set(self.calls.get() + 1);
        let mut bytes = self.source.snapshot_chunk(request)?;
        if let Some(cancel) = &self.cancel {
            cancel.cancel();
        }
        if self.corrupt {
            bytes[0] ^= 1;
        }
        Some(bytes)
    }
}

fn installed_floor(path: &Path) -> LogIndex {
    let mut header = [0; 8];
    File::open(path.join("current"))
        .unwrap()
        .read_exact(&mut header)
        .unwrap();
    LogIndex(u64::from_le_bytes(header))
}

// This fixture's schema is an opaque blob, with the revision from its Raft
// descriptor. A real decoder must also validate its own schema/embedded index.
// The caller serializes installs, owns staging cleanup and destination bounds,
// and would recover after any publication I/O error with an unknown outcome.
fn install(
    path: &Path,
    snapshot: &RaftSnapshot,
    source: &dyn SnapshotChunkSource,
    cancellation: SnapshotCancellation,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut reader = SnapshotPayloadReader::new(
        snapshot,
        source,
        installed_floor(path),
        SnapshotReadOptions::new(LENGTH).with_chunk_bytes(CHUNK),
        cancellation,
    )?;
    let stage_path = path.join("staging");
    let mut staged = File::create(&stage_path)?;
    staged.write_all(&snapshot.metadata.last_included_index.0.to_le_bytes())?;
    // Fixed decoder buffer; neither source, reader nor destination holds the
    // complete 2 MiB payload. Failed reads leave only an unpublished staging file.
    let mut bytes = [0; 113];
    loop {
        let count = reader.read(&mut bytes)?;
        if count == 0 {
            break;
        }
        staged.write_all(&bytes[..count])?;
    }
    let verified = reader.finish()?;
    assert_eq!(verified.snapshot(), snapshot);
    assert_eq!(verified.applied_index(), LogIndex(42));
    staged.sync_all()?;
    drop(staged);
    fs::rename(stage_path, path.join("current"))?;
    File::open(path)?.sync_all()?;
    Ok(())
}

#[test]
fn file_backed_reader_publishes_only_verified_state_and_floor_then_reopens() {
    let scratch = ScratchDir::new("bounded-snapshot-reader");
    let (store, snapshot) = source(&scratch.path().join("raft"));
    let mut old = 7_u64.to_le_bytes().to_vec();
    old.extend_from_slice(b"old state");
    fs::write(scratch.path().join("current"), &old).unwrap();

    for corrupt in [false, true] {
        let token = SnapshotCancellation::new();
        let observed = Observed {
            source: &store,
            calls: Cell::new(0),
            cancel: (!corrupt).then(|| token.clone()),
            corrupt,
        };
        let error = install(scratch.path(), &snapshot, &observed, token).unwrap_err();
        let error = error.downcast_ref::<io::Error>().unwrap();
        assert_eq!(
            error.kind(),
            if corrupt {
                io::ErrorKind::InvalidData
            } else {
                io::ErrorKind::Other
            }
        );
        assert_eq!(observed.calls.get(), if corrupt { 513 } else { 1 });
        assert_eq!(fs::read(scratch.path().join("current")).unwrap(), old);
        assert_eq!(installed_floor(scratch.path()), LogIndex(7));
    }

    // A fresh attempt truncates the abandoned staging file. The immutable Raft
    // snapshot remains reusable after cancellation or a faulty read wrapper.
    let observed = Observed {
        source: &store,
        calls: Cell::new(0),
        cancel: None,
        corrupt: false,
    };
    install(
        scratch.path(),
        &snapshot,
        &observed,
        SnapshotCancellation::new(),
    )
    .unwrap();
    assert_eq!(observed.calls.get(), 513);
    assert_eq!(installed_floor(scratch.path()), LogIndex(42));
    let mut reopened = File::open(scratch.path().join("current")).unwrap();
    assert_eq!(reopened.metadata().unwrap().len(), LENGTH + 8);
    let mut header = [0; 8];
    reopened.read_exact(&mut header).unwrap();
    assert_eq!(u64::from_le_bytes(header), 42);
    let mut bytes = [0; 113];
    let mut length = 0;
    loop {
        let count = reopened.read(&mut bytes).unwrap();
        if count == 0 {
            break;
        }
        assert!(bytes[..count].iter().all(|byte| *byte == 0x5a));
        length += count as u64;
    }
    assert_eq!(length, LENGTH);
}
