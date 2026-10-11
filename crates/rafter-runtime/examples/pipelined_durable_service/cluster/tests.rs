//! Recovery dispatch stays bounded across protocol-only committed prefixes.

use super::*;
use rafter::{LogEntryKind, Term};
use rafter_storage::{
    durable_batch::WalRaftNodeStores, PersistedRaftLogEntry, RaftHardState, RaftHardStateStore,
    RaftLogSegment,
};

#[test]
fn recovery_routes_a_long_noop_prefix_as_one_application_item() {
    let root = std::env::temp_dir().join(format!("rafter-range-recovery-{}", std::process::id()));
    assert!(
        !root.exists(),
        "recovery requires a fresh scenario directory"
    );
    let directory = super::super::storage::node_dir(&root, NodeId(1));
    std::fs::create_dir_all(&directory).unwrap();
    let (mut hard, mut log, snapshots) = WalRaftNodeStores::open(&directory).unwrap().into_parts();
    // Multiple leadership terms committed only their protocol no-op. The first
    // application command is beyond the worker's default 64-item batch bound.
    let mut entries: Vec<_> = (1..=130)
        .map(|index| PersistedRaftLogEntry {
            index: LogIndex(index),
            term: Term(index),
            kind: LogEntryKind::Noop,
        })
        .collect();
    entries.push(PersistedRaftLogEntry {
        index: LogIndex(131),
        term: Term(130),
        kind: LogEntryKind::Application(encode_set("recovered", "range").into()),
    });
    log.append_entries(&entries).unwrap();
    hard.write_hard_state(RaftHardState {
        current_term: Term(130),
        voted_for: None,
        commit_index: LogIndex(131),
        committed_configuration: None,
    })
    .unwrap();
    drop((hard, log, snapshots));

    let mut cluster = Cluster::open(root.clone());
    cluster.drain_application(NodeId(1));
    let state = application::snapshot(&cluster.replicas[&NodeId(1)].state);
    assert_eq!(state.applied, LogIndex(131));
    assert_eq!(state.kv.get("recovered"), Some(&String::from("range")));
    assert_eq!(cluster.restart(NodeId(1)), LogIndex(131));
    assert_eq!(
        application::snapshot(&cluster.replicas[&NodeId(1)].state).kv,
        state.kv
    );
    cluster.shutdown();
    std::fs::remove_dir_all(root).unwrap();
}
