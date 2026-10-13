//! File-backed compaction pulls a payload far larger than application credits.

use super::{
    super::*,
    support::{adapt, config, log_with_commands, Store},
};
use crate::DurableRaftNodeStorage;
use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion, LogIndex,
    RaftSnapshot, RaftSnapshotMetadata, SnapshotChunkRequest, SnapshotChunkSource, SnapshotGroupId,
};
use rafter_storage::{FileRaftSnapshotStore, RaftSnapshotStore};
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

// CRC32 of exactly 2 MiB + 17 bytes of 0x5a, independently generated in chunks.
const PAYLOAD_LEN: u64 = 2 * 1024 * 1024 + 17;
const PAYLOAD_CRC32: u32 = 0xed60_5685;

#[derive(Debug, Default)]
struct Stats {
    requests: AtomicUsize,
    largest: AtomicUsize,
    bytes: AtomicUsize,
}
#[derive(Debug)]
struct PatternSource {
    snapshot: RaftSnapshot,
    stats: Arc<Stats>,
}
impl SnapshotChunkSource for PatternSource {
    fn snapshot_chunk(&self, request: SnapshotChunkRequest<'_>) -> Option<Vec<u8>> {
        if request.transfer_id != self.snapshot.transfer_id()
            || request.metadata != &self.snapshot.metadata
            || request.total_payload_len != PAYLOAD_LEN
            || request.application_payload_crc32 != PAYLOAD_CRC32
            || request.offset.checked_add(u64::from(request.len))? > PAYLOAD_LEN
        {
            return None;
        }
        self.stats.requests.fetch_add(1, Ordering::SeqCst);
        self.stats
            .largest
            .fetch_max(request.len as usize, Ordering::SeqCst);
        self.stats
            .bytes
            .fetch_add(request.len as usize, Ordering::SeqCst);
        assert!(
            request.len <= 256 * 1024,
            "publisher requested an unbounded buffer"
        );
        Some(vec![0x5a; request.len as usize])
    }
}
struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "rafter-owner-stream-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn file_compaction_streams_above_worker_budget_and_reopens_the_exact_descriptor() {
    let scratch = Scratch::new();
    let storage = log_with_commands(3).into_storage();
    let mut options = ServiceOptions::new();
    options.application = options.application.with_limits(1, 64, 1, 64);
    let mut owner = DurableService::recover(
        config(),
        DurableRaftNodeStorage {
            hard_state_store: storage.hard_state_store,
            log_segment: storage.log_segment,
            snapshot_store: FileRaftSnapshotStore::open(&scratch.0).unwrap(),
        },
        Store::default(),
        options,
        adapt,
    )
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while owner.state() != ServiceState::Ready {
        assert!(Instant::now() < deadline);
        assert!(matches!(
            owner.poll().unwrap(),
            Some(ServiceEvent::Applied(_)) | None
        ));
        std::thread::yield_now();
    }
    let stats = Arc::new(Stats::default());
    let retired = owner
        .compact_snapshot_with(|app, node| {
            let metadata = RaftSnapshotMetadata::new(
                SnapshotGroupId::new("owner-stream")?,
                node.id(),
                app.floor,
                node.term_at_index(app.floor).unwrap(),
                node.current_term(),
                ApplicationSnapshotMetadata::new(
                    ApplicationSnapshotKind::new("pattern")?,
                    ApplicationSnapshotVersion::new(1)?,
                ),
            )?;
            let snapshot = RaftSnapshot::new(metadata, PAYLOAD_LEN, PAYLOAD_CRC32);
            let source = PatternSource {
                snapshot: snapshot.clone(),
                stats: Arc::clone(&stats),
            };
            Ok((snapshot, source))
        })
        .unwrap();
    assert!(!retired.is_empty());
    assert_eq!(stats.requests.load(Ordering::SeqCst), 9);
    assert_eq!(stats.largest.load(Ordering::SeqCst), 256 * 1024);
    assert_eq!(
        stats.bytes.load(Ordering::SeqCst),
        usize::try_from(PAYLOAD_LEN).unwrap()
    );
    assert_eq!(owner.durable_through(), Some(LogIndex(4)));
    let descriptor = owner.ready_node().unwrap().snapshot().unwrap().clone();
    // Runtime stamps committed membership, while source requests retained its original identity.
    assert!(descriptor.metadata.committed_configuration.is_some());
    owner.begin_shutdown();
    drop(owner.finish_shutdown().unwrap());
    drop(owner);
    let reopened = FileRaftSnapshotStore::open(&scratch.0).unwrap();
    assert_eq!(reopened.current_snapshot(), Some(descriptor.clone()));
    assert_eq!(
        reopened.snapshot_chunk(SnapshotChunkRequest {
            transfer_id: descriptor.transfer_id(),
            metadata: &descriptor.metadata,
            total_payload_len: PAYLOAD_LEN,
            application_payload_crc32: PAYLOAD_CRC32,
            offset: PAYLOAD_LEN - 17,
            len: 17,
        }),
        Some(vec![0x5a; 17])
    );
}
