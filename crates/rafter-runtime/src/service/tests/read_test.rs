//! Lifecycle and verified-floor boundaries for borrowed service reads.

use super::{super::*, lifecycle_test::snapshotted, support::*};
use crate::application::ApplicationReadRejection;
use rafter::{Input, LogIndex, ReadId};
use std::{cell::Cell, panic::AssertUnwindSafe, rc::Rc, time::Duration};

fn refused(service: &mut Owner, state: ServiceState) {
    let error = service
        .try_read_application(LogIndex::ZERO, |_| panic!("closed service executed query"))
        .unwrap_err();
    assert_eq!(error.rejection(), ServiceReadRejection::NotReady(state));
}

#[test]
fn recovery_and_floor_refusals_preserve_the_owned_callback_for_retry() {
    let mut service = owner(log_with_commands(2), Store::default(), 1);
    refused(&mut service, ServiceState::Recovering);
    let called = Rc::new(Cell::new(false));
    let observed = Rc::clone(&called);
    let input = String::from("owned query");
    let query = move |store: &Store| {
        called.set(true);
        (input, store.applied.clone())
    };
    let error = service
        .try_read_application(LogIndex(4), query)
        .unwrap_err();
    assert_eq!(
        error.rejection(),
        ServiceReadRejection::NotReady(ServiceState::Recovering)
    );
    assert!(!observed.get());
    until(&mut service, |s| s.state() == ServiceState::Ready);
    let error = service
        .try_read_application(LogIndex(4), error.into_query())
        .unwrap_err();
    assert!(!observed.get());
    assert_eq!(
        error.rejection(),
        ServiceReadRejection::Application(ApplicationReadRejection::Behind {
            required: LogIndex(4),
            durable_through: LogIndex(3)
        })
    );
    assert_eq!(service.state(), ServiceState::Ready);
    let read = service
        .try_read_application(LogIndex(3), error.into_query())
        .unwrap();
    assert!(observed.get());
    assert_eq!(read.durable_through(), LogIndex(3));
    assert_eq!(
        read.into_output(),
        (String::from("owned query"), vec![LogIndex(2), LogIndex(3)])
    );
    assert_eq!(service.state(), ServiceState::Ready);
}

#[test]
fn snapshot_handoff_and_shutdown_close_reads_until_a_valid_live_incarnation_exists() {
    let mut service = owner(snapshotted(), Store::default(), 1);
    let Some(ServiceEvent::InstallSnapshot {
        mut application, ..
    }) = service.poll().unwrap()
    else {
        panic!("snapshot handoff required");
    };
    refused(&mut service, ServiceState::InstallingSnapshot);
    application.floor = LogIndex(4);
    service.resume_snapshot(application).unwrap();
    refused(&mut service, ServiceState::Recovering);
    until(&mut service, |s| s.state() == ServiceState::Ready);
    let read = service
        .try_read_application(LogIndex(5), |store| store.floor)
        .unwrap();
    assert_eq!(read.durable_through(), LogIndex(5));
    assert_eq!(read.into_output(), LogIndex(5));
    service.begin_shutdown();
    refused(&mut service, ServiceState::Draining);
    assert_eq!(service.finish_shutdown().unwrap().floor, LogIndex(5));
    refused(&mut service, ServiceState::Stopped);
}

#[test]
fn undispatched_output_refuses_reads_until_the_owner_is_ready() {
    let mut service = empty();
    until(&mut service, |s| s.state() == ServiceState::Ready);
    service.try_step(Input::Tick.into()).unwrap();
    until(&mut service, |s| s.state() == ServiceState::Ready);
    service
        .try_step(Input::ReadIndex { read_id: ReadId(5) }.into())
        .unwrap();
    refused(&mut service, ServiceState::Busy);
    until(&mut service, |s| s.state() == ServiceState::Ready);
    let read = service
        .try_read_application(LogIndex::ZERO, |store| store.floor)
        .unwrap();
    assert_eq!(read.into_output(), LogIndex::ZERO);
}

#[test]
fn ready_owner_returns_busy_during_application_io_then_reads_the_completed_view() {
    let mut store = Store::default();
    let (started, release) = gate(&mut store);
    let mut service = owner(log_with_commands(0), store, 1);
    let mut release = release;
    until(&mut service, |s| s.state() == ServiceState::Ready);
    service.try_step(Input::Tick.into()).unwrap();
    until(&mut service, |s| s.state() == ServiceState::Ready);
    service
        .try_step(
            Input::ClientProposal {
                payload: b"write".to_vec(),
            }
            .into(),
        )
        .unwrap();
    until(&mut service, |s| s.state() == ServiceState::Ready);
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    let error = service
        .try_read_application(LogIndex::ZERO, |store| store.applied.clone())
        .unwrap_err();
    assert_eq!(
        error.rejection(),
        ServiceReadRejection::Application(ApplicationReadRejection::Busy)
    );
    assert_eq!(
        service.state(),
        ServiceState::Ready,
        "transient refusal must not poison"
    );
    release.now();
    until(&mut service, DurableService::idle);
    let read = service
        .try_read_application(LogIndex(3), error.into_query())
        .unwrap();
    assert_eq!(read.durable_through(), LogIndex(3));
    assert_eq!(read.into_output(), vec![LogIndex(3)]);
}

#[test]
fn caught_query_panic_terminally_fences_every_owner_operation() {
    let mut service = empty();
    until(&mut service, |s| s.state() == ServiceState::Ready);
    // Returning an ordinary query error leaves the owner usable.
    let read = service
        .try_read_application(LogIndex::ZERO, |_| Err::<(), _>("query refusal"))
        .unwrap();
    assert_eq!(read.into_output(), Err("query refusal"));
    assert_eq!(service.state(), ServiceState::Ready);
    let panic = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let _ = service.try_read_application(LogIndex::ZERO, |_| panic!("query panic"));
    }));
    assert!(panic.is_err());
    assert_eq!(service.state(), ServiceState::Failed);
    refused(&mut service, ServiceState::Failed);
    assert!(matches!(service.poll_next(), Err(ServiceError::Failed)));
    assert!(matches!(
        service.try_step(Input::Tick.into()),
        Err(ServiceStepError::Unavailable {
            state: ServiceState::Failed,
            ..
        })
    ));
    service.begin_shutdown();
    assert!(matches!(
        service.finish_shutdown(),
        Err(ServiceError::Failed)
    ));
}
