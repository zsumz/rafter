//! Real file images at publication cuts, including a second crash in recovery.

use super::*;
use crate::tests::file_backed_fixture::TestDirectory;
use rafter_storage::{
    FileRaftHardStateStore, FileRaftLogSegment, FileRaftNodeStores, FileRaftSnapshotStore,
};
use std::path::Path;

type FileNode = DurableRaftNode<FileRaftHardStateStore, FileRaftLogSegment, FileRaftSnapshotStore>;

#[test]
fn configuration_recovery_survives_every_file_crash_cut() {
    for change in CHANGES {
        for batched in [false, true] {
            let directory = TestDirectory::new("configuration-control");
            let mutations = install(directory.path(), change, batched, None);
            for after in 1..=mutations.len() {
                let directory = TestDirectory::new("configuration-crash");
                let cuts = install(directory.path(), change, batched, Some(after));
                assert_eq!(cuts, mutations[..after]);
                let published = after == mutations.len();
                let mut reopened = open(directory.path());
                if !published {
                    assert_old_hard_state(&reopened.hard_state_store);
                }
                assert_recovered(&mut reopened, change, &cuts, published);
                reopened
                    .step(RaftInput::Tick)
                    .expect("normal step publishes recovered identity");
                drop(reopened);
                let mut again = open(directory.path());
                assert_recovered(&mut again, change, &cuts, published);
                progress::finish_recovery(&mut again, change);
            }
        }
    }
}

#[test]
fn configuration_snapshot_recovers_after_a_second_file_crash() {
    for matching in [false, true] {
        let change = Change::Snapshot { matching };
        for batched in [false, true] {
            let directory = TestDirectory::new("configuration-recovery-control");
            let staged = install(directory.path(), change, batched, Some(2));
            assert_eq!(staged, [Mutation::HardState, Mutation::Stage]);
            let mutations = recover(directory.path(), None);
            assert!(mutations.contains(&Mutation::Promote));
            assert!(mutations.contains(&Mutation::Compact));
            for after in 1..=mutations.len() {
                let directory = TestDirectory::new("configuration-recovery-crash");
                install(directory.path(), change, batched, Some(2));
                assert_eq!(recover(directory.path(), Some(after)), mutations[..after]);
                let mut reopened = open(directory.path());
                assert_recovered(&mut reopened, change, &staged, false);
                reopened.step(RaftInput::Tick).unwrap();
                drop(reopened);
                let mut again = open(directory.path());
                assert_recovered(&mut again, change, &staged, false);
                progress::finish_recovery(&mut again, change);
            }
        }
    }
}

fn install(path: &Path, change: Change, batched: bool, after: Option<usize>) -> Vec<Mutation> {
    let (mut hard_state, mut log, snapshots) = FileRaftNodeStores::open(path).unwrap().into_parts();
    initialize(&mut hard_state, &mut log, change);
    let cut = Cut::new(after);
    let mut runtime = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        cut.wrap(hard_state),
        cut.wrap(log),
        cut.wrap(snapshots),
    )
    .unwrap();
    drive(&mut runtime, change, batched, after.is_some());
    cut.mutations()
}

fn recover(path: &Path, after: Option<usize>) -> Vec<Mutation> {
    let (hard_state, log, snapshots) = FileRaftNodeStores::open(path).unwrap().into_parts();
    let cut = Cut::new(after);
    let result = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        cut.wrap(hard_state),
        cut.wrap(log),
        cut.wrap(snapshots),
    );
    if after.is_some() {
        assert!(result.is_err());
    } else {
        assert!(result.is_ok());
    }
    cut.mutations()
}

pub(super) fn open(path: &Path) -> FileNode {
    let (hard_state, log, snapshots) = FileRaftNodeStores::open(path).unwrap().into_parts();
    DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        hard_state,
        log,
        snapshots,
    )
    .expect("configuration crash image reopens from real files")
}
