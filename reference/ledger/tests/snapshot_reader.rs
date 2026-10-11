//! An external opaque-blob application installs through the public reader.
//!
//! Real snapshot files feed a private staging file; only a verified receipt
//! permits publication of bytes and applied floor together. This tests normal
//! reopen and failed attempts, not power-loss guarantees of the host filesystem.

#[path = "support/scratch.rs"]
mod scratch;

use std::{
    cell::Cell,
    fs::{self, File},
    io::{self, Read, Write},
    path::Path,
};

use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion, LogIndex,
    NodeId, RaftSnapshot, RaftSnapshotMetadata, SnapshotChunkRequest, SnapshotChunkSource,
    SnapshotGroupId, Term,
};
use rafter_runtime::snapshot_io::{
    SnapshotCancellation, SnapshotPayloadReader, SnapshotReadOptions,
};
use rafter_storage::{FileRaftSnapshotStore, RaftSnapshotStore};
use scratch::ScratchDir;

// Independently calculated IEEE CRC32 of exactly this constant byte sequence.
const LENGTH: u64 = 2 * 1024 * 1024 + 17;
const CRC32: u32 = 0xed60_5685;
const CHUNK: u32 = 4096;

struct Pattern(RaftSnapshot);
impl SnapshotChunkSource for Pattern {
    fn snapshot_chunk(&self, request: SnapshotChunkRequest<'_>) -> Option<Vec<u8>> {
        assert_eq!(request.transfer_id, self.0.transfer_id());
        assert_eq!(request.metadata, &self.0.metadata);
        assert_eq!(request.total_payload_len, LENGTH);
        assert_eq!(request.application_payload_crc32, CRC32);
        assert!(request.offset + u64::from(request.len) <= LENGTH);
        assert!(request.len <= 256 * 1024);
        Some(vec![0x5a; request.len as usize])
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
    let snapshot = RaftSnapshot::new(metadata, LENGTH, CRC32);
    let mut store = FileRaftSnapshotStore::open(path).unwrap();
    store
        .write_snapshot_from_source(&snapshot, &Pattern(snapshot.clone()))
        .unwrap();
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
