//! Application backpressure, read fences, and terminal failure checks.

use super::{super::*, support::*};
use crate::application::ApplicationFailureKind;
use rafter::{ClientProposalInput, Input, LocalProposalId, LogIndex, Output, ReadId};
use std::time::Duration;

#[test]
fn stalled_application_keeps_tail_owned_and_shutdown_delivers_every_completion() {
    let mut store = Store::default();
    let (started, release) = gate(&mut store);
    let mut service = owner(log_with_commands(9), store, 1);
    // Drop order matters: the gate guard releases before service joins on unwind.
    let mut release = release;
    assert!(service.poll().unwrap().is_none());
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    for _ in 0..100 {
        assert!(service.poll().unwrap().is_none());
    }
    assert_eq!(service.durable_through(), Some(LogIndex::ZERO));
    let input = ServiceInput::from(Input::Tick);
    assert!(
        matches!(service.try_step(input.clone()), Err(ServiceStepError::Unavailable { input: returned, .. }) if returned == input)
    );
    service.begin_shutdown();
    assert!(matches!(
        service.finish_shutdown(),
        Err(ServiceError::NotReady(ServiceState::Draining))
    ));
    release.now();
    let events = until(&mut service, DurableService::idle);
    let applied: Vec<_> = events
        .into_iter()
        .flat_map(|event| match event {
            ServiceEvent::Applied(done) => done
                .into_parts()
                .0
                .into_iter()
                .map(|e| e.last)
                .collect::<Vec<_>>(),
            other => panic!("unexpected event: {other:?}"),
        })
        .collect();
    assert_eq!(applied, (2..=10).map(LogIndex).collect::<Vec<_>>());
    let store = service.finish_shutdown().unwrap();
    assert_eq!(store.applied, applied);
    assert_eq!(service.state(), ServiceState::Stopped);
    assert!(service.finish_shutdown().is_err());
}

#[test]
fn read_grant_waits_for_application_but_noop_barrier_needs_no_phantom_apply() {
    let mut service = empty();
    until(&mut service, |s| s.state() == ServiceState::Ready);
    service.try_step(ServiceInput::from(Input::Tick)).unwrap();
    until(&mut service, |s| s.state() == ServiceState::Ready);
    service
        .try_step(ServiceInput::from(Input::ReadIndex { read_id: ReadId(1) }))
        .unwrap();
    let events = until(&mut service, |s| s.state() == ServiceState::Ready);
    assert!(events.iter().any(|e| matches!(
        e,
        ServiceEvent::Output(Output::ReadIndexGranted {
            read_index: LogIndex(1),
            ..
        })
    )));
    service.begin_shutdown();
    let mut store = service.finish_shutdown().unwrap();
    let (started, release) = gate(&mut store);
    // Reopen the real durable consensus image, then elect in the next term.
    let storage = service.ready_node().unwrap().clone().into_storage();
    let mut service =
        Owner::recover(config(), storage, store, ServiceOptions::new(), adapt).unwrap();
    let mut release = release;
    service.poll().unwrap();
    service.try_step(ServiceInput::from(Input::Tick)).unwrap();
    until(&mut service, |s| s.state() == ServiceState::Ready);
    service
        .try_step(ServiceInput::Proposals(vec![ClientProposalInput {
            proposal_id: Some(LocalProposalId(7)),
            payload: b"write".to_vec(),
        }]))
        .unwrap();
    until(&mut service, |s| s.state() == ServiceState::Ready);
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    service
        .try_step(ServiceInput::from(Input::ReadIndex { read_id: ReadId(2) }))
        .unwrap();
    for _ in 0..100 {
        assert!(service.poll().unwrap().is_none());
    }
    release.now();
    let events = until(&mut service, DurableService::idle);
    assert!(
        matches!(events.first(), Some(ServiceEvent::Applied(done)) if done.outcomes() == [Some(LocalProposalId(7))])
    );
    assert!(matches!(
        events.last(),
        Some(ServiceEvent::Output(Output::ReadIndexGranted {
            read_id: ReadId(2),
            ..
        }))
    ));
}

#[test]
fn application_failure_and_wrong_durable_floor_fence_every_later_input() {
    for wrong_floor in [false, true] {
        let store = Store {
            fail: !wrong_floor,
            wrong_floor,
            ..Store::default()
        };
        let mut service = owner(log_with_commands(3), store, 2);
        let events = until(&mut service, |s| s.state() == ServiceState::Failed);
        assert!(events
            .iter()
            .all(|e| !matches!(e, ServiceEvent::Applied(_))));
        assert!(
            matches!(events.last(), Some(ServiceEvent::ApplicationFailed(failure)) if matches!(failure.kind(), ApplicationFailureKind::Store(_) | ApplicationFailureKind::DurableFloor { .. }))
        );
        assert!(matches!(service.poll(), Err(ServiceError::Failed)));
        assert!(matches!(
            service.try_step(ServiceInput::from(Input::Tick)),
            Err(ServiceStepError::Unavailable {
                state: ServiceState::Failed,
                ..
            })
        ));
    }
}

#[test]
fn application_panic_and_adapter_range_violation_are_terminal() {
    let mut service = owner(
        log_with_commands(1),
        Store {
            panic: true,
            ..Store::default()
        },
        1,
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        assert!(std::time::Instant::now() < deadline);
        match service.poll() {
            Err(ServiceError::ApplicationStopped) => break,
            Ok(None) => std::thread::yield_now(),
            other => panic!("unexpected {other:?}"),
        }
    }
    assert_eq!(service.state(), ServiceState::Failed);
    let mut service = Owner::recover(
        config(),
        log_with_commands(1).into_storage(),
        Store::default(),
        ServiceOptions::new(),
        |command| {
            let mut entry = adapt(command).unwrap();
            entry.first = entry.last;
            Ok(entry)
        },
    )
    .unwrap();
    assert!(matches!(service.poll(), Err(ServiceError::AdapterRange)));
    assert_eq!(service.state(), ServiceState::Failed);
}

#[test]
fn oversized_application_item_fails_without_an_endless_full_retry() {
    let mut options = ServiceOptions::new();
    options.application = options.application.with_limits(1, 32, 1, 128);
    let mut service = Owner::recover(
        config(),
        log_with_commands(1).into_storage(),
        Store::default(),
        options,
        adapt,
    )
    .unwrap();
    assert!(matches!(
        service.poll(),
        Err(ServiceError::ApplicationAdmission(_))
    ));
    assert_eq!(service.state(), ServiceState::Failed);
}

#[test]
fn adapter_decode_error_and_caught_panic_leave_no_skipped_command_live() {
    let mut service = Owner::recover(
        config(),
        log_with_commands(1).into_storage(),
        Store::default(),
        ServiceOptions::new(),
        |_command| Err(std::io::Error::other("malformed command").into()),
    )
    .unwrap();
    assert!(matches!(service.poll(), Err(ServiceError::Adapter(_))));
    assert_eq!(service.state(), ServiceState::Failed);
    let mut service = Owner::recover(
        config(),
        log_with_commands(1).into_storage(),
        Store::default(),
        ServiceOptions::new(),
        |_command| panic!("injected adapter panic"),
    )
    .unwrap();
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| service.poll())).is_err());
    assert_eq!(service.state(), ServiceState::Failed);
    assert!(matches!(service.poll(), Err(ServiceError::Failed)));
}
