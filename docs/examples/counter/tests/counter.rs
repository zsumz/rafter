//! Durable application behavior independent of the cluster transport.
#[path = "../src/counter.rs"]
mod counter;
#[path = "../src/disk.rs"]
mod disk;

use counter::{Counter, State};
use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion, LogIndex,
    NodeId, RaftSnapshotMetadata, SnapshotGroupId, Term,
};
use rafter_app::state_machine::{
    ApplicationSnapshot, ApplyBatch, ApplyEntry, ReadBarrier, ReplicatedStateMachine,
};
use rafter_storage::{FileRaftSnapshotStore, PersistedRaftSnapshot, RaftSnapshotStore};
use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

struct Scratch(PathBuf);
impl Scratch {
    fn new(label: &str) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory =
            std::env::temp_dir().join(format!("counter-{label}-{}-{nonce}", std::process::id()));
        fs::create_dir_all(&directory).unwrap();
        disk::save(&directory.join("counter"), &State::default()).unwrap();
        Self(directory)
    }
    fn open(&self) -> Counter {
        Counter::open(&self.0).unwrap()
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn increment(index: u64, amount: u64) -> ApplyBatch<u64> {
    ApplyBatch {
        entries: vec![ApplyEntry {
            index: LogIndex(index),
            term: Term(1),
            command: amount,
            local_proposal_id: None,
        }],
    }
}

#[test]
fn acknowledged_values_and_progress_survive_reopen() {
    let scratch = Scratch::new("reopen");
    let mut counter = scratch.open();
    assert_eq!(
        counter
            .decode_command(&counter.encode_command(&5).unwrap())
            .unwrap(),
        5
    );
    assert!(counter.decode_command(&[0; 7]).is_err());
    assert_eq!(counter.apply_batch(increment(2, 5)).unwrap()[0].result, 5);
    assert_eq!(counter.apply_batch(increment(3, 3)).unwrap()[0].result, 8);
    drop(counter);
    let mut recovered = scratch.open();
    assert_eq!(recovered.value(), 8);
    assert_eq!(recovered.applied_index().unwrap(), LogIndex(3));
    assert!(recovered.apply_batch(increment(3, 3)).is_err());
    assert!(recovered
        .read(
            (),
            ReadBarrier {
                required_applied_index: LogIndex(4),
                local_applied_index: LogIndex(3)
            }
        )
        .is_err());
    assert_eq!(scratch.open().value(), 8);
}

#[test]
fn failed_publication_does_not_advance_memory_or_disk() {
    let scratch = Scratch::new("failed-write");
    let mut counter = scratch.open();
    counter.apply_batch(increment(2, 5)).unwrap();
    fs::create_dir(scratch.0.join("counter.tmp")).unwrap();
    assert!(counter.apply_batch(increment(3, 3)).is_err());
    assert_eq!(counter.value(), 5);
    assert_eq!(counter.applied_index().unwrap(), LogIndex(2));
    assert_eq!(scratch.open().value(), 5);
}

#[test]
fn inline_and_runtime_stored_snapshots_restore_the_same_counter() {
    let source = Scratch::new("source");
    let mut counter = source.open();
    counter.apply_batch(increment(2, 5)).unwrap();
    let snapshot = counter.build_snapshot(LogIndex(2)).unwrap();
    assert!(counter.build_snapshot(LogIndex(3)).is_err());
    let inline = Scratch::new("inline");
    inline.open().install_snapshot(snapshot.clone()).unwrap();
    assert_eq!(inline.open().value(), 5);

    let promoted = Scratch::new("promoted");
    let mut store = FileRaftSnapshotStore::open(promoted.0.join("raft/snapshots")).unwrap();
    let metadata = RaftSnapshotMetadata::new(
        SnapshotGroupId::new("counter").unwrap(),
        NodeId(1),
        LogIndex(2),
        Term(1),
        Term(1),
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("counter-v1").unwrap(),
            ApplicationSnapshotVersion::new(1).unwrap(),
        ),
    )
    .unwrap();
    store
        .write_snapshot(PersistedRaftSnapshot {
            metadata,
            application_payload: snapshot.payload,
        })
        .unwrap();
    let descriptor = store.current_snapshot().unwrap();
    drop(store);
    promoted
        .open()
        .install_snapshot(ApplicationSnapshot {
            applied_index: LogIndex(2),
            payload: Vec::new(),
            raft_snapshot: Some(descriptor),
        })
        .unwrap();
    assert_eq!(promoted.open().value(), 5);
    assert_eq!(promoted.open().applied_index().unwrap(), LogIndex(2));
    let conflict = ApplicationSnapshot {
        applied_index: LogIndex(2),
        payload: serde_json::to_vec(&State {
            applied: 2,
            value: 99,
        })
        .unwrap(),
        raft_snapshot: None,
    };
    assert!(promoted.open().install_snapshot(conflict).is_err());
    assert_eq!(promoted.open().value(), 5);
}
