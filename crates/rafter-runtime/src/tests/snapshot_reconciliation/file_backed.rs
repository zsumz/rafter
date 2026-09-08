//! Real store files survive installation failures and a second crash in recovery.

use super::*;
use crate::tests::file_backed_fixture::TestDirectory;
use rafter_storage::FileRaftNodeStores;
use std::path::Path;

#[test]
fn file_backed_snapshot_reconciliation_survives_every_crash_cut() {
    for matching in [false, true] {
        for batched in [false, true] {
            let directory = TestDirectory::new("snapshot-install-success");
            let mutations = file_install(directory.path(), matching, batched, None);
            for after in 1..=mutations.len() {
                let directory = TestDirectory::new("snapshot-install-cut");
                let observed = file_install(directory.path(), matching, batched, Some(after));
                assert_eq!(observed, mutations[..after]);
                let (hard_state, log, snapshots) = FileRaftNodeStores::open(directory.path())
                    .expect("file image opens after failed mutation")
                    .into_parts();
                if after < mutations.len() {
                    assert_eq!(hard_state.current().commit_index, LogIndex::ZERO);
                    assert_eq!(hard_state.current().committed_configuration, None);
                }
                let mut reopened = DurableRaftNode::with_storage_and_snapshot_store(
                    raft_config(2, &[1, 3]),
                    hard_state,
                    log,
                    snapshots,
                )
                .unwrap_or_else(|error| panic!("matching={matching}, cut {observed:?}: {error}"));
                if after == 1 {
                    assert_eq!(reopened.snapshot_index(), LogIndex::ZERO);
                    assert_eq!(reopened.last_log_index(), LogIndex(3));
                    assert!(reopened.drain_committed_outputs().is_empty());
                } else {
                    assert_recovered(&mut reopened, matching);
                    drop(reopened);
                    reopen_and_check(directory.path(), matching);
                }
            }
        }
    }
}

#[test]
fn file_backed_snapshot_reconciliation_recovers_after_recovery_crashes() {
    for matching in [false, true] {
        for batched in [false, true] {
            let directory = TestDirectory::new("snapshot-recovery-success");
            // The second mutation is the complete staged transfer. Reopen must
            // finish exactly the installation the failed step could not finish.
            assert_eq!(
                file_install(directory.path(), matching, batched, Some(2)),
                vec![Mutation::HardState, Mutation::Stage],
            );
            let mutations = recover_with_cut(directory.path(), None);
            assert!(mutations.contains(&Mutation::Promote));
            assert!(mutations.contains(&Mutation::Compact));
            for after in 1..=mutations.len() {
                let directory = TestDirectory::new("snapshot-recovery-cut");
                file_install(directory.path(), matching, batched, Some(2));
                assert_eq!(
                    recover_with_cut(directory.path(), Some(after)),
                    mutations[..after],
                );
                reopen_and_check(directory.path(), matching);
                reopen_and_check(directory.path(), matching);
            }
        }
    }
}

fn file_install(path: &Path, matching: bool, batched: bool, after: Option<usize>) -> Vec<Mutation> {
    let (mut hard_state, mut log, snapshots) = FileRaftNodeStores::open(path)
        .expect("file stores open")
        .into_parts();
    initialize(&mut hard_state, &mut log);
    let cut = Cut::new(after);
    let mut runtime = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        cut.wrap(hard_state),
        cut.wrap(log),
        cut.wrap(snapshots),
    )
    .expect("file follower hydrates");
    drive(&mut runtime, matching, batched, after.is_some());
    cut.mutations()
}

fn recover_with_cut(path: &Path, after: Option<usize>) -> Vec<Mutation> {
    let (hard_state, log, snapshots) = FileRaftNodeStores::open(path)
        .expect("interrupted file stores open")
        .into_parts();
    let cut = Cut::new(after);
    let result = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        cut.wrap(hard_state),
        cut.wrap(log),
        cut.wrap(snapshots),
    );
    if after.is_some() {
        assert!(result.is_err(), "recovery reaches the armed cut");
    } else {
        assert!(result.is_ok(), "recovery completes");
    }
    cut.mutations()
}

fn reopen_and_check(path: &Path, matching: bool) {
    let (hard_state, log, snapshots) = FileRaftNodeStores::open(path)
        .expect("file stores reopen after recovery")
        .into_parts();
    let mut runtime = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        hard_state,
        log,
        snapshots,
    )
    .expect("file runtime recovers again");
    assert_recovered(&mut runtime, matching);
}
