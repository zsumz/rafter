//! Every in-memory mutation boundary, then a normal step and another reopen.

use super::*;

type Image = DurableRaftNodeStorage<
    InMemoryRaftHardStateStore,
    InMemoryRaftLogSegment,
    InMemoryRaftSnapshotStore,
>;

#[test]
fn configuration_recovery_survives_every_memory_crash_cut() {
    for change in CHANGES {
        for batched in [false, true] {
            let (mutations, _) = install(change, batched, None);
            assert_eq!(mutations.first(), Some(&Mutation::HardState));
            assert_eq!(mutations.last(), Some(&Mutation::HardState));
            for after in 1..=mutations.len() {
                let (cuts, image) = install(change, batched, Some(after));
                assert_eq!(cuts, mutations[..after]);
                let published = after == mutations.len();
                if !published {
                    assert_old_hard_state(&image.hard_state_store);
                }
                let mut reopened = open(image);
                assert_recovered(&mut reopened, change, &cuts, published);
                reopened
                    .step(RaftInput::Tick)
                    .expect("normal step after recovery");
                let mut again = open(reopened.into_storage());
                assert_recovered(&mut again, change, &cuts, published);
                progress::finish_recovery(&mut again, change);
            }
        }
    }
}

fn install(change: Change, batched: bool, after: Option<usize>) -> (Vec<Mutation>, Image) {
    let mut hard_state = InMemoryRaftHardStateStore::new();
    let mut log = InMemoryRaftLogSegment::new();
    initialize(&mut hard_state, &mut log, change);
    let cut = Cut::new(after);
    let mut runtime = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        cut.wrap(hard_state),
        cut.wrap(log),
        cut.wrap(InMemoryRaftSnapshotStore::new()),
    )
    .unwrap();
    drive(&mut runtime, change, batched, after.is_some());
    let image = runtime.into_storage();
    (
        cut.mutations(),
        DurableRaftNodeStorage {
            hard_state_store: image.hard_state_store.inner,
            log_segment: image.log_segment.inner,
            snapshot_store: image.snapshot_store.inner,
        },
    )
}

fn open(image: Image) -> DurableRaftNode {
    DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        image.hard_state_store,
        image.log_segment,
        image.snapshot_store,
    )
    .expect("every accepted configuration publication boundary reopens")
}
