//! Shutdown cannot finish while snapshot installation owns the application.
use super::support::*;
use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion, NodeConfig,
    RaftSnapshotMetadata, SnapshotGroupId,
};
use rafter_runtime::{
    service::{ServiceEvent, ServiceOptions},
    DurableRaftNode,
};
use rafter_service::durable::{DurableClientOptions, DurableReadLimits, DurableWriteLimits};
use rafter_storage::{InMemoryRaftHardStateStore, PersistedRaftSnapshot};
use std::{
    num::NonZeroUsize,
    time::{Duration, Instant},
};

#[test]
fn shutdown_waits_for_snapshot_handoff_and_preserves_the_installed_metrics_floor() {
    let floor = LogIndex(2);
    let config = NodeConfig::new(NodeId(1), vec![], 1).unwrap();
    let mut core = DurableRaftNode::new(config.clone(), InMemoryRaftHardStateStore::new()).unwrap();
    core.step(Input::Tick).unwrap();
    core.step(Input::ClientProposal { payload: vec![9] })
        .unwrap();
    let metadata = RaftSnapshotMetadata::new(
        SnapshotGroupId::new("durable-group").unwrap(),
        core.id(),
        floor,
        core.term_at_index(floor).unwrap(),
        core.current_term(),
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("counter").unwrap(),
            ApplicationSnapshotVersion::new(1).unwrap(),
        ),
    )
    .unwrap();
    core.compact_log_with_snapshot(PersistedRaftSnapshot {
        metadata,
        application_payload: vec![9],
    })
    .unwrap();
    let dropped = Arc::new(AtomicBool::new(false));
    let count = NonZeroUsize::new(2).unwrap();
    let bytes = NonZeroUsize::new(64).unwrap();
    let owner = crate::write_support::Owner::<u8>::recover_with_reads(
        config,
        core.into_storage(),
        App {
            floor: LogIndex::ZERO,
            value: 0,
            gate: None,
            dropped: Arc::clone(&dropped),
        },
        DurableClientOptions::new(
            ServiceOptions::new(),
            DurableWriteLimits {
                writes: count,
                payload_bytes: bytes,
            },
            DurableReadLimits {
                reads: count,
                payload_bytes: bytes,
            },
        ),
        |c| {
            Ok(Entry {
                first: c.first_index,
                index: c.index,
                value: c.payload[0],
            })
        },
        |app, _| app.value,
        || {},
    )
    .unwrap();
    let mut driver = Driver::attach(GROUP, owner, Link::default()).unwrap();
    let tx = driver.sender();
    let metrics = tx.metrics(GROUP).unwrap();
    let mut read = tx.read(
        GROUP,
        vec![0],
        ReadConsistency::Local,
        ReadOptions::default(),
    );
    let ServicePoll::Event(ServiceEvent::InstallSnapshot {
        snapshot,
        mut application,
    }) = driver.poll_next().unwrap()
    else {
        panic!("missing handoff");
    };
    assert_eq!(snapshot.metadata.last_included_index, floor);
    let mut shutdown = tx.shutdown(GROUP);
    assert!(matches!(driver.poll_next().unwrap(), ServicePoll::Pending));
    assert!(poll(&mut shutdown).is_pending());
    assert!(poll(&mut read).is_pending());
    assert!(!dropped.load(Ordering::Acquire));
    assert_eq!(metrics.current().applied_index, LogIndex::ZERO);
    application.floor = floor;
    application.value = 9;
    driver.resume_snapshot(application).unwrap();
    assert_eq!(metrics.current().applied_index, floor);
    let deadline = Instant::now() + Duration::from_secs(5);
    while driver.state() != ServiceState::Stopped {
        driver.poll_next().unwrap();
        assert!(Instant::now() < deadline);
    }
    assert!(dropped.load(Ordering::Acquire));
    assert!(matches!(poll(&mut shutdown), Poll::Ready(Ok(()))));
    assert!(matches!(
        poll(&mut read),
        Poll::Ready(Err(ReadError::ShuttingDown))
    ));
}
