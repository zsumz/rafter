//! Maintenance errors and caught panics must close admission without early fate.
use super::support::*;
use std::{
    panic::{catch_unwind, AssertUnwindSafe},
    task::Wake,
};

#[test]
fn builder_error_and_application_floor_mutation_close_both_owner_endpoints() {
    for mutate_floor in [false, true] {
        let mut node = ready_owner();
        let writes = node.owner.sender();
        let reads = node.sender.clone();
        let mut write = writes.write(vec![9], WriteOptions::default());
        let mut read = node.read(ReadConsistency::Local);
        let result = node.owner.compact_snapshot_with(|app, core| {
            if mutate_floor {
                let candidate = snapshot(app, core)?;
                app.floor = LogIndex(3);
                Ok(candidate)
            } else {
                Err(std::io::Error::other("injected snapshot builder failure").into())
            }
        });
        if mutate_floor {
            assert!(matches!(
                result,
                Err(ServiceError::ApplicationFloor {
                    expected: LogIndex(2),
                    actual: LogIndex(3)
                })
            ));
        } else {
            assert!(matches!(result, Err(ServiceError::SnapshotBuild(_))));
        }
        assert_eq!(node.owner.state(), ServiceState::Failed);
        assert!(matches!(
            poll(&mut writes.write(vec![8], WriteOptions::default())),
            Poll::Ready(Err(WriteError::ShuttingDown))
        ));
        assert!(matches!(
            poll(&mut reads.read(vec![0], ReadConsistency::Local, ReadOptions::default())),
            Poll::Ready(Err(ReadError::ShuttingDown))
        ));
        assert!(poll(&mut write).is_pending());
        assert!(poll(&mut read).is_pending());
        assert!(matches!(
            node.owner
                .compact_snapshot_with::<Source>(|_, _| panic!("revived failed owner")),
            Err(ServiceError::Failed)
        ));
        drop(node);
        assert!(matches!(
            poll(&mut write),
            Poll::Ready(Err(WriteError::ShuttingDown))
        ));
        assert!(matches!(
            poll(&mut read),
            Poll::Ready(Err(ReadError::ShuttingDown))
        ));
    }
}

#[test]
fn caught_maintenance_panics_also_fence_the_unwrapped_owner() {
    for source_drop in [false, true] {
        let mut node = ready_owner();
        let writes = node.owner.sender();
        let mut write = writes.write(vec![9], WriteOptions::default());
        let mut read = node.read(ReadConsistency::Local);
        assert!(catch_unwind(AssertUnwindSafe(|| {
            node.owner.compact_snapshot_with(|app, core| {
                assert!(source_drop, "injected builder panic");
                let (descriptor, mut source) = snapshot(app, core)?;
                source.panic_drop = true;
                Ok((descriptor, source))
            })
        }))
        .is_err());
        assert_eq!(node.owner.state(), ServiceState::Failed);
        assert!(matches!(
            poll(&mut writes.write(vec![8], WriteOptions::default())),
            Poll::Ready(Err(WriteError::ShuttingDown))
        ));
        assert!(matches!(
            poll(&mut node.read(ReadConsistency::Local)),
            Poll::Ready(Err(ReadError::ShuttingDown))
        ));
        assert!(poll(&mut write).is_pending());
        assert!(poll(&mut read).is_pending());
        drop(node);
        assert!(matches!(
            poll(&mut write),
            Poll::Ready(Err(WriteError::ShuttingDown))
        ));
        assert!(matches!(
            poll(&mut read),
            Poll::Ready(Err(ReadError::ShuttingDown))
        ));
    }
}

#[test]
fn caught_builder_and_source_drop_panics_fence_the_group_without_early_fates() {
    for source_drop in [false, true] {
        let mut node = ready();
        let tx = node.driver.sender();
        let mut write = tx.write(GROUP, vec![9], WriteOptions::default());
        let mut read = tx.read(
            GROUP,
            vec![0],
            ReadConsistency::Local,
            ReadOptions::default(),
        );
        let result = catch_unwind(AssertUnwindSafe(|| {
            node.driver.compact_snapshot_with(|app, core| {
                assert!(source_drop, "injected builder panic");
                let (descriptor, mut source) = snapshot(app, core)?;
                source.panic_drop = true;
                Ok((descriptor, source))
            })
        }));
        assert!(result.is_err());
        assert_eq!(node.driver.state(), ServiceState::Failed);
        assert_eq!(
            node.driver.ready_node().unwrap().snapshot_index(),
            if source_drop {
                LogIndex(2)
            } else {
                LogIndex::ZERO
            }
        );
        assert!(matches!(
            poll(&mut tx.write(GROUP, vec![8], WriteOptions::default())),
            Poll::Ready(Err(WriteError::ShuttingDown))
        ));
        assert!(matches!(
            poll(&mut tx.read(
                GROUP,
                vec![0],
                ReadConsistency::Local,
                ReadOptions::default()
            )),
            Poll::Ready(Err(ReadError::ShuttingDown))
        ));
        assert!(matches!(
            poll(&mut tx.transfer_leadership(GROUP, NodeId(1))),
            Poll::Ready(Err(TransferLeadershipError::ShuttingDown))
        ));
        assert!(poll(&mut write).is_pending());
        assert!(poll(&mut read).is_pending());
        let mut stop = tx.shutdown(GROUP);
        assert!(poll(&mut stop).is_pending());
        drop(node);
        assert!(matches!(
            poll(&mut write),
            Poll::Ready(Err(WriteError::ShuttingDown))
        ));
        assert!(matches!(
            poll(&mut read),
            Poll::Ready(Err(ReadError::ShuttingDown))
        ));
        assert!(matches!(poll(&mut stop), Poll::Ready(Ok(()))));
    }
}

struct PanicUpdate;
impl Wake for PanicUpdate {
    fn wake(self: Arc<Self>) {
        panic!("injected post-compaction metrics wake panic");
    }
}

#[test]
fn metrics_panic_after_snapshot_publication_fences_the_group() {
    let mut node = ready();
    let tx = node.driver.sender();
    let mut watch = tx.metrics(GROUP).unwrap();
    let mut changed = Box::pin(watch.changed());
    let waker = Waker::from(Arc::new(PanicUpdate));
    assert!(changed
        .as_mut()
        .poll(&mut Context::from_waker(&waker))
        .is_pending());
    let mut write = tx.write(GROUP, vec![9], WriteOptions::default());
    let result = catch_unwind(AssertUnwindSafe(|| {
        node.driver.compact_snapshot_with(snapshot)
    }));
    assert!(result.is_err());
    assert_eq!(node.driver.state(), ServiceState::Failed);
    assert_eq!(
        node.driver.ready_node().unwrap().snapshot_index(),
        LogIndex(2)
    );
    assert!(matches!(
        poll(&mut tx.transfer_leadership(GROUP, NodeId(1))),
        Poll::Ready(Err(TransferLeadershipError::ShuttingDown))
    ));
    assert!(matches!(
        poll(&mut tx.read(
            GROUP,
            vec![0],
            ReadConsistency::Local,
            ReadOptions::default()
        )),
        Poll::Ready(Err(ReadError::ShuttingDown))
    ));
    assert!(poll(&mut write).is_pending());
    drop(changed);
    drop(node);
    assert!(matches!(
        poll(&mut write),
        Poll::Ready(Err(WriteError::ShuttingDown))
    ));
}
