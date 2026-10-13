//! Reject incompatible snapshot identities before publishing application state.
#[path = "../src/counter.rs"]
mod counter;
#[path = "../src/disk.rs"]
mod disk;

use counter::{Counter, State};
use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion, LogIndex,
    NodeId, RaftSnapshotMetadata, SnapshotGroupId, Term,
};
use rafter_app::state_machine::{ApplicationSnapshot, ReplicatedStateMachine};
use rafter_storage::{FileRaftSnapshotStore, PersistedRaftSnapshot, RaftSnapshotStore};
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};

struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "counter-schema-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        disk::save(
            &path.join("counter"),
            &State {
                applied: 2,
                value: 5,
            },
        )
        .unwrap();
        Self(path)
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

fn refuses(group: &str, kind: &str, version: u16, boundary: u64) {
    let scratch = Scratch::new();
    let before = fs::read(scratch.0.join("counter")).unwrap();
    let mut app = scratch.open();
    let metadata = RaftSnapshotMetadata::new(
        SnapshotGroupId::new(group).unwrap(),
        NodeId(1),
        LogIndex(boundary),
        Term(1),
        Term(1),
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new(kind).unwrap(),
            ApplicationSnapshotVersion::new(version).unwrap(),
        ),
    )
    .unwrap();
    let mut store = FileRaftSnapshotStore::open(scratch.0.join("raft/snapshots")).unwrap();
    store
        .write_snapshot(PersistedRaftSnapshot {
            metadata,
            application_payload: serde_json::to_vec(&State {
                applied: 3,
                value: 8,
            })
            .unwrap(),
        })
        .unwrap();
    let snapshot = ApplicationSnapshot {
        applied_index: LogIndex(3),
        payload: Vec::new(),
        raft_snapshot: store.current_snapshot(),
    };
    drop(store);
    let result = app.install_snapshot(snapshot);
    assert!(
        result.is_err(),
        "accepted incompatible snapshot {group}/{kind}/{version} at {boundary}"
    );
    assert_eq!(app.applied_index().unwrap(), LogIndex(2));
    assert_eq!(app.value(), 5);
    assert_eq!(fs::read(scratch.0.join("counter")).unwrap(), before);
    assert_eq!(scratch.open().value(), 5);
    assert!(!scratch.0.join("counter.tmp").exists());
}

#[test]
fn rejects_another_application_kind() {
    refuses("counter", "another-application", 1, 3);
}
#[test]
fn rejects_an_unsupported_application_version() {
    refuses("counter", "counter-v1", 2, 3);
}
#[test]
fn rejects_another_snapshot_group() {
    refuses("another-group", "counter-v1", 1, 3);
}
#[test]
fn rejects_a_descriptor_floor_that_disagrees_with_the_application_wrapper() {
    refuses("counter", "counter-v1", 1, 4);
}

#[test]
fn inline_snapshot_limit_is_checked_before_publication() {
    let scratch = Scratch::new();
    let mut app = scratch.open();
    let before = fs::read(scratch.0.join("counter")).unwrap();
    let mut payload = serde_json::to_vec(&State {
        applied: 3,
        value: 8,
    })
    .unwrap();
    payload.resize(65_537, b' '); // Valid JSON with trailing whitespace.
    assert!(app
        .install_snapshot(ApplicationSnapshot {
            applied_index: LogIndex(3),
            payload: payload.clone(),
            raft_snapshot: None
        })
        .is_err());
    assert_eq!(app.value(), 5);
    assert_eq!(app.applied_index().unwrap(), LogIndex(2));
    assert_eq!(fs::read(scratch.0.join("counter")).unwrap(), before);
    assert!(!scratch.0.join("counter.tmp").exists());
    payload.pop();
    app.install_snapshot(ApplicationSnapshot {
        applied_index: LogIndex(3),
        payload,
        raft_snapshot: None,
    })
    .unwrap();
    assert_eq!(app.value(), 8);
    assert_eq!(scratch.open().applied_index().unwrap(), LogIndex(3));
}

#[test]
fn startup_rejects_incompatible_stored_snapshots_before_transport_activation() {
    for (group, kind, version, diagnostic) in [
        (
            "another-group",
            "counter-v1",
            1,
            "counter snapshot belongs to another group",
        ),
        (
            "counter",
            "another-application",
            1,
            "unsupported counter snapshot application format",
        ),
        (
            "counter",
            "counter-v1",
            2,
            "unsupported counter snapshot application format",
        ),
    ] {
        let scratch = Scratch::new();
        let binary = env!("CARGO_BIN_EXE_rafter-counter");
        let initialized = std::process::Command::new(binary)
            .arg("init")
            .current_dir(&scratch.0)
            .output()
            .unwrap();
        assert!(initialized.status.success(), "{initialized:?}");
        let path = scratch.0.join("data/node-1");
        let before = fs::read(path.join("counter")).unwrap();
        let metadata = RaftSnapshotMetadata::new(
            SnapshotGroupId::new(group).unwrap(),
            NodeId(1),
            LogIndex(3),
            Term(1),
            Term(1),
            ApplicationSnapshotMetadata::new(
                ApplicationSnapshotKind::new(kind).unwrap(),
                ApplicationSnapshotVersion::new(version).unwrap(),
            ),
        )
        .unwrap();
        let mut store = FileRaftSnapshotStore::open(path.join("raft/snapshots")).unwrap();
        store
            .write_snapshot(PersistedRaftSnapshot {
                metadata,
                application_payload: serde_json::to_vec(&State {
                    applied: 3,
                    value: 8,
                })
                .unwrap(),
            })
            .unwrap();
        drop(store);
        // No certificates exist: the identity refusal must precede TLS setup.
        let rejected = std::process::Command::new(binary)
            .args(["node", "1"])
            .current_dir(&scratch.0)
            .output()
            .unwrap();
        assert!(!rejected.status.success());
        assert!(
            String::from_utf8_lossy(&rejected.stderr).contains(diagnostic),
            "{rejected:?}"
        );
        assert_eq!(fs::read(path.join("counter")).unwrap(), before);
    }
}
