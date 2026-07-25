use std::path::Path;

use rafter::{BootstrapValidationError, LogIndex, NodeId};
use rafter_invariant_test::{oracle_assert, oracle_assert_eq};
use rafter_runtime::RaftRuntimeError;
use rafter_storage::{FileRaftNodeStores, PersistedRaftLogEntry, RaftLogSegment};

use super::support::{
    commit, expected_kv, fresh_elected_node, remove_test_root, test_root, write_command,
};
use crate::{
    app::{persist_app_state, AppState},
    runtime::open_application_node,
};

#[test]
pub(super) fn ps04_production_open_rejects_app_floor_beyond_commit() {
    let root = test_root("ps04-floor-beyond-commit");
    let mut opened = fresh_elected_node(&root);
    let (commit_index, _) = commit(&mut opened.node, &write_command(1, 0));
    let current_term = opened.node.current_term();
    drop(opened);

    let uncommitted_index = LogIndex(commit_index.0 + 1);
    // Every part shares the directory-ownership guard, so the log handle must
    // drop before production open can acquire the directory again.
    {
        let (_, mut log, _) = FileRaftNodeStores::open(root.join("raft"))
            .expect("file-backed stores reopen")
            .into_parts();
        log.append_entries(&[PersistedRaftLogEntry::application(
            uncommitted_index,
            current_term,
            serde_json::to_vec(&write_command(2, 1)).expect("command encodes"),
        )])
        .expect("uncommitted durable tail appends");
    }
    persist_invalid_floor(&root, uncommitted_index);

    let error = open_application_node(&root, NodeId(1), Vec::new()).err();
    oracle_assert!(
        error.is_some(),
        "production open must reject an application floor beyond commit"
    );
    let runtime_error = *error
        .expect("typed oracle established that production open failed")
        .downcast::<RaftRuntimeError>()
        .expect("production open must preserve the typed runtime error");
    oracle_assert_eq!(
        runtime_error,
        RaftRuntimeError::Bootstrap(BootstrapValidationError::AppliedFloorBeyondCommit {
            applied_through: uncommitted_index,
            commit_index,
        }),
    );
    remove_test_root(root);
}

#[test]
pub(super) fn ps04_production_open_rejects_app_floor_beyond_log_coverage() {
    let root = test_root("ps04-floor-beyond-log");
    let mut opened = fresh_elected_node(&root);
    let (last_log_index, _) = commit(&mut opened.node, &write_command(1, 0));
    drop(opened);

    let beyond_log = LogIndex(last_log_index.0 + 1);
    persist_invalid_floor(&root, beyond_log);

    let error = open_application_node(&root, NodeId(1), Vec::new()).err();
    oracle_assert!(
        error.is_some(),
        "production open must reject an application floor beyond log coverage"
    );
    let runtime_error = *error
        .expect("typed oracle established that production open failed")
        .downcast::<RaftRuntimeError>()
        .expect("production open must preserve the typed runtime error");
    oracle_assert_eq!(
        runtime_error,
        RaftRuntimeError::Bootstrap(BootstrapValidationError::AppliedFloorBeyondLog {
            applied_through: beyond_log,
            last_log_index,
        }),
    );
    remove_test_root(root);
}

fn persist_invalid_floor(root: &Path, applied: LogIndex) {
    persist_app_state(
        root,
        &AppState {
            applied,
            kv: expected_kv(1),
        },
    )
    .expect("invalid app floor persists for reopen test");
}
