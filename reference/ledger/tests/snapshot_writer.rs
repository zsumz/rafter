//! Actual ledger schema creation through the public bounded staging API.

#[path = "support/scratch.rs"]
mod scratch;
#[path = "support/snapshot_writer.rs"]
mod setup;
#[path = "support/snapshot_source.rs"]
mod snapshot_source;
#[allow(dead_code)]
mod support;

use std::io::{self, Write};

use rafter::LogIndex;
use rafter_app::state_machine::{ApplicationSnapshot, ReplicatedStateMachine};
use rafter_reference_ledger::{store::LedgerStore, DurableLedgerStateMachine};
use rafter_runtime::snapshot_io::SnapshotCancellation;
use rafter_storage::{FileRaftSnapshotStore, RaftSnapshotStore};

use scratch::ScratchDir;
use setup::{golden, large, metadata, small};
use snapshot_source::FileSource;

#[derive(Default)]
struct Counted {
    calls: usize,
    max: usize,
    bytes: u64,
    flushes: usize,
}
impl Write for Counted {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.calls += 1;
        self.max = self.max.max(bytes.len());
        self.bytes += u64::try_from(bytes.len()).unwrap();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        self.flushes += 1;
        Ok(())
    }
}

#[test]
fn streamed_and_inline_encoders_preserve_independent_version_one_bytes() {
    let mut app = small();
    let (bytes, snapshot) = app
        .write_snapshot_to(
            metadata(LogIndex(7)),
            Vec::new(),
            SnapshotCancellation::new(),
        )
        .unwrap();
    assert_eq!(bytes, golden());
    assert_eq!(app.build_snapshot(LogIndex(7)).unwrap().payload, bytes);
    assert_eq!(snapshot.metadata, metadata(LogIndex(7)));
    assert_eq!(snapshot.application_payload_len, 62);
    // Independent Python zlib.crc32 over the version-1 fixture.
    assert_eq!(snapshot.application_payload_crc32, 0xc218_aa5b);
}

#[test]
fn large_ledger_writes_bounded_chunks_without_retaining_an_encoded_image() {
    let app = large(131_072);
    let at = LogIndex(262_146);
    assert_eq!(app.applied_index().unwrap(), at);
    let (sink, snapshot) = app
        .write_snapshot_to(
            metadata(at),
            Counted::default(),
            SnapshotCancellation::new(),
        )
        .unwrap();
    // Independent schema length and CRC32: 131072 (id, balance=1) rows in
    // ascending order, supply 131072, floor 262146, one client at epoch 2.
    assert_eq!(sink.bytes, 2_097_198);
    assert_eq!(snapshot.application_payload_len, sink.bytes);
    assert_eq!(snapshot.application_payload_crc32, 0xaa5a_82f8);
    assert!(sink.calls >= 129 && sink.calls <= 130);
    assert!(sink.max <= 16 * 1024);
    assert_eq!(sink.flushes, 1);
}

struct Partial {
    bytes: Vec<u8>,
    interrupt: bool,
}
impl Write for Partial {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if std::mem::take(&mut self.interrupt) {
            return Err(io::ErrorKind::Interrupted.into());
        }
        let count = bytes.len().min(3);
        self.bytes.extend_from_slice(&bytes[..count]);
        Ok(count)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn partial_writes_and_interrupted_retry_produce_the_same_descriptor() {
    let (sink, snapshot) = small()
        .write_snapshot_to(
            metadata(LogIndex(7)),
            Partial {
                bytes: Vec::new(),
                interrupt: true,
            },
            SnapshotCancellation::new(),
        )
        .unwrap();
    assert_eq!(sink.bytes, golden());
    assert_eq!(snapshot.application_payload_len, 62);
    assert_eq!(snapshot.application_payload_crc32, 0xc218_aa5b);
}

#[test]
fn fsynced_stream_promotes_installs_and_reopens_as_the_same_ledger() {
    let app = large(2048);
    let at = app.applied_index().unwrap();
    let source = FileSource::build(|file| {
        let (_, snapshot) =
            app.write_snapshot_to(metadata(at), file, SnapshotCancellation::new())?;
        Ok(snapshot)
    })
    .unwrap();
    let scratch = ScratchDir::new("ledger-streamed-writer-install");
    let raft_path = scratch.path().join("snapshots");
    let snapshot = source.snapshot().clone();
    let mut store = FileRaftSnapshotStore::open(&raft_path).unwrap();
    store
        .write_snapshot_from_source(&snapshot, &source)
        .unwrap();
    drop(source); // The promoted store owns the only remaining payload file.
    drop(store);
    let store = LedgerStore::open(scratch.path(), support::config(1, 2048)).unwrap();
    let mut restored = DurableLedgerStateMachine::new(store, raft_path);
    restored
        .install_snapshot(ApplicationSnapshot {
            applied_index: at,
            payload: Vec::new(),
            raft_snapshot: Some(snapshot),
        })
        .unwrap();
    assert_eq!(restored.ledger().view(), app.ledger().view());
    drop(restored);
    let reopened = LedgerStore::open(scratch.path(), support::config(1, 2048)).unwrap();
    assert_eq!(reopened.applied_index(), at);
    assert_eq!(reopened.ledger().view(), app.ledger().view());
}
