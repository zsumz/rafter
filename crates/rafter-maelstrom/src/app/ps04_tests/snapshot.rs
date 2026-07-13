use super::support::{
    cas_command, commit, expected_kv, fresh_elected_node, initialize_process, load_persisted_app,
    persist_with_interruption, remove_test_root, test_root, write_command,
};
use crate::{app::AppState, raft::snapshots::compact_application_snapshot};

#[test]
pub(super) fn ps04_snapshot_persist_crash_reopens_snapshot_then_dispatches_committed_suffix() {
    let root = test_root("ps04-snapshot-persist-recovery");
    let mut opened = fresh_elected_node(&root);
    let commands = [
        write_command(1, 0),
        write_command(2, 1),
        cas_command(3, 1, 2),
    ];
    let committed = commands
        .iter()
        .map(|command| commit(&mut opened.node, command))
        .collect::<Vec<_>>();

    persist_with_interruption(&root, &mut opened.app, &committed[0]);
    let snapshot_app = AppState {
        applied: committed[1].0,
        kv: expected_kv(1),
    };
    let snapshot_index = compact_application_snapshot(&mut opened.node, &snapshot_app)
        .expect("production Maelstrom snapshot persists and compacts");
    assert_eq!(snapshot_index, committed[1].0);
    let snapshot = opened.node.snapshot().expect("durable snapshot installs");
    let committed_membership = opened.node.committed_membership();
    assert_eq!(
        snapshot.metadata.committed_membership(),
        Some(&committed_membership),
        "snapshot must retain the runtime-normalized committed membership"
    );
    drop(opened);

    let stale_app = load_persisted_app(&root);
    assert_eq!(stale_app.applied, committed[0].0);
    assert_eq!(stale_app.kv, expected_kv(0));

    let process = initialize_process(&root);
    let initialized = process.initialized.as_ref().expect("node initializes");
    assert_eq!(initialized.node.snapshot_index(), snapshot_index);
    assert_eq!(initialized.app.applied, committed[2].0);
    assert_eq!(
        initialized.app.kv,
        expected_kv(2),
        "snapshot payload must install before the CAS suffix is dispatched"
    );
    let reopened_membership = initialized.node.committed_membership();
    assert_eq!(
        initialized
            .node
            .snapshot()
            .expect("snapshot remains installed")
            .metadata
            .committed_membership(),
        Some(&reopened_membership)
    );

    let persisted = load_persisted_app(&root);
    assert_eq!(persisted.applied, committed[2].0);
    assert_eq!(persisted.kv, expected_kv(2));
    remove_test_root(root);
}
