//! Local snapshot maintenance observes both durability and output-consumption fences.

use super::{super::*, support::*};
use crate::RaftRuntimeError;
use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion, Input,
    LogIndex, RaftSnapshot, RaftSnapshotMetadata, SnapshotChunkRequest, SnapshotChunkSource,
    SnapshotGroupId, Term,
};
use rafter_storage::{InMemoryRaftSnapshotStore, PersistedRaftSnapshot, RaftSnapshotStore};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, Arc,
    },
    time::Duration,
};

#[derive(Debug)]
pub(super) struct Source {
    inner: InMemoryRaftSnapshotStore,
    calls: Arc<AtomicUsize>,
    missing: bool,
    corrupt: bool,
}
impl SnapshotChunkSource for Source {
    fn snapshot_chunk(&self, request: SnapshotChunkRequest<'_>) -> Option<Vec<u8>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.missing {
            None
        } else {
            let mut bytes = self.inner.snapshot_chunk(request)?;
            if self.corrupt && !bytes.is_empty() {
                bytes[0] ^= 1;
            }
            Some(bytes)
        }
    }
}
pub(super) fn snapshot(
    store: &mut Store,
    node: &Node,
) -> Result<(RaftSnapshot, Source), ServiceAdapterError> {
    let metadata = RaftSnapshotMetadata::new(
        SnapshotGroupId::new("owner-compaction")?,
        node.id(),
        store.floor,
        node.term_at_index(store.floor).unwrap(),
        node.current_term(),
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("indexes")?,
            ApplicationSnapshotVersion::new(1)?,
        ),
    )?;
    let bytes = store
        .applied
        .iter()
        .flat_map(|index| index.0.to_le_bytes())
        .collect();
    let inner = InMemoryRaftSnapshotStore::with_snapshot(PersistedRaftSnapshot {
        metadata,
        application_payload: bytes,
    });
    Ok((
        inner.current_snapshot().unwrap(),
        Source {
            inner,
            calls: Arc::default(),
            missing: false,
            corrupt: false,
        },
    ))
}
fn ready() -> Owner {
    let mut owner = owner(log_with_commands(3), Store::default(), 1);
    until(&mut owner, |s| s.state() == ServiceState::Ready);
    owner
}

#[test]
fn compaction_returns_retired_entries_and_restarts_application_without_reapplying() {
    let mut service = ready();
    let retired = service.compact_snapshot_with(snapshot).unwrap();
    assert!(!retired.is_empty());
    assert!(retired.payload_bytes() > 0);
    assert_eq!(service.ready_node().unwrap().snapshot_index(), LogIndex(4));
    assert_eq!(service.durable_through(), Some(LogIndex(4)));
    assert_eq!(service.state(), ServiceState::Ready);
    let read = service
        .try_read_application(LogIndex(4), |store| store.applied.clone())
        .unwrap();
    assert_eq!(read.durable_through(), LogIndex(4));
    assert_eq!(read.into_output(), [LogIndex(2), LogIndex(3), LogIndex(4)]);
    service.try_step(Input::Tick.into()).unwrap();
    until(&mut service, |s| s.state() == ServiceState::Ready);
    service
        .try_step(
            Input::ClientProposal {
                payload: b"after compaction".to_vec(),
            }
            .into(),
        )
        .unwrap();
    let events = until(&mut service, DurableService::idle);
    assert!(
        matches!(events.as_slice(), [ServiceEvent::Applied(done)] if done.entries()[0].first == LogIndex(5) && done.entries()[0].last == LogIndex(6))
    );
    service.begin_shutdown();
    assert_eq!(
        service.finish_shutdown().unwrap().applied,
        [LogIndex(2), LogIndex(3), LogIndex(4), LogIndex(6)]
    );
}

#[test]
fn finished_but_unconsumed_application_work_still_refuses_compaction() {
    let mut store = Store::default();
    let (started, release) = gate(&mut store);
    let (notify, notified) = mpsc::sync_channel(1);
    let mut options = ServiceOptions::new();
    options.application = options.application.with_limits(1, 64, 1, 64);
    let mut service = Owner::recover_with_wake(
        config(),
        log_with_commands(0).into_storage(),
        store,
        options,
        adapt,
        move || {
            let _ = notify.try_send(());
        },
    )
    .unwrap();
    let mut release = release;
    assert!(matches!(
        service.compact_snapshot_with(snapshot),
        Err(ServiceError::NotReady(ServiceState::Recovering))
    ));
    until(&mut service, |s| s.state() == ServiceState::Ready);
    service.try_step(Input::Tick.into()).unwrap();
    until(&mut service, |s| s.state() == ServiceState::Ready);
    service
        .try_step(Input::ClientProposal { payload: vec![1] }.into())
        .unwrap();
    until(&mut service, |s| s.state() == ServiceState::Ready);
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(matches!(
        service.compact_snapshot_with::<Source>(|_, _| panic!("must not call builder")),
        Err(ServiceError::NotQuiescent)
    ));
    release.now();
    // The floor is published before the completion is queued. The worker wake
    // establishes that the event is actually available without consuming it.
    notified.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(service.durable_through(), Some(LogIndex(3)));
    assert!(matches!(
        service.compact_snapshot_with::<Source>(|_, _| panic!("completion not consumed")),
        Err(ServiceError::NotQuiescent)
    ));
    assert!(matches!(
        service.poll().unwrap(),
        Some(ServiceEvent::Applied(_))
    ));
    drop(service.compact_snapshot_with(snapshot).unwrap());
    service.begin_shutdown();
    assert!(matches!(
        service.compact_snapshot_with::<Source>(|_, _| panic!("shutdown is closed")),
        Err(ServiceError::NotReady(ServiceState::Draining))
    ));
}

#[test]
fn invalid_floor_and_term_write_nothing_and_keep_the_application_usable() {
    for wrong_term in [false, true] {
        let mut service = ready();
        let calls = Arc::new(AtomicUsize::new(0));
        let result = service.compact_snapshot_with(|application, node| {
            let (mut descriptor, mut source) = snapshot(application, node)?;
            source.calls = Arc::clone(&calls);
            if wrong_term {
                descriptor.metadata.last_included_term = Term(99);
                descriptor.metadata.hard_state_term = Term(99);
            } else {
                descriptor.metadata.last_included_index = LogIndex(3);
            }
            Ok((descriptor, source))
        });
        assert!(matches!(
            result,
            Err(ServiceError::SnapshotFloor { .. }
                | ServiceError::Snapshot(RaftRuntimeError::SnapshotBoundaryTermMismatch { .. }))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            service.ready_node().unwrap().snapshot_index(),
            LogIndex::ZERO
        );
        assert_eq!(service.state(), ServiceState::Ready);
        drop(service.compact_snapshot_with(snapshot).unwrap());
    }
}

#[test]
fn builder_error_floor_mutation_and_panic_fence_without_publishing() {
    let mut service = ready();
    assert!(matches!(
        service
            .compact_snapshot_with::<Source>(|_, _| Err(
                std::io::Error::other("snapshot failed").into()
            )),
        Err(ServiceError::SnapshotBuild(_))
    ));
    assert_eq!(service.state(), ServiceState::Failed);
    assert_eq!(
        service.ready_node().unwrap().snapshot_index(),
        LogIndex::ZERO
    );
    let mut service = ready();
    assert!(matches!(
        service.compact_snapshot_with(|app, node| {
            let result = snapshot(app, node);
            app.floor = LogIndex(5);
            result
        }),
        Err(ServiceError::ApplicationFloor { .. })
    ));
    assert_eq!(service.state(), ServiceState::Failed);
    assert_eq!(
        service.ready_node().unwrap().snapshot_index(),
        LogIndex::ZERO
    );
    let mut service = ready();
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
        || service.compact_snapshot_with::<Source>(|_, _| panic!("builder panics"))
    ))
    .is_err());
    assert_eq!(service.state(), ServiceState::Failed);
    assert!(service.try_step(Input::Tick.into()).is_err());
}

#[test]
fn missing_chunks_or_bad_checksum_fence_the_live_service() {
    for missing in [false, true] {
        let mut service = ready();
        let result = service.compact_snapshot_with(|app, node| {
            let (descriptor, mut source) = snapshot(app, node)?;
            source.missing = missing;
            source.corrupt = !missing;
            Ok((descriptor, source))
        });
        assert!(
            matches!(
                result,
                Err(ServiceError::Snapshot(RaftRuntimeError::SnapshotWrite(
                    rafter_storage::RaftSnapshotStoreWriteError::SourceChunkUnavailable { .. }
                ))) if missing
            ) || matches!(result,
                Err(ServiceError::Snapshot(RaftRuntimeError::SnapshotWrite(
                    rafter_storage::RaftSnapshotStoreWriteError::SnapshotPayloadChecksumMismatch { .. }
                ))) if !missing
            )
        );
        assert_eq!(service.state(), ServiceState::Failed);
        assert!(service.poll().is_err());
        assert!(service.try_step(Input::Tick.into()).is_err());
    }
}
