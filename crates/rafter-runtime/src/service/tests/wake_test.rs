//! Retained notifications drive recovery and failures without periodic polling.

use super::{super::*, support::*};
use rafter::{Input, LogIndex};
use std::{sync::mpsc, time::Duration};

pub(super) fn notified(node: Node, store: Store, credits: usize) -> (Owner, mpsc::Receiver<()>) {
    let (notify, wake) = mpsc::sync_channel(1);
    let mut options = ServiceOptions::new();
    options.application = options
        .application
        .with_limits(credits, credits * 64, 1, 64);
    let owner = Owner::recover_with_wake(
        config(),
        node.into_storage(),
        store,
        options,
        adapt,
        move || {
            // Capacity one deliberately coalesces concurrent and repeated wakes.
            let _ = notify.try_send(());
        },
    )
    .unwrap();
    (owner, wake)
}

pub(super) fn drain(owner: &mut Owner) -> Vec<Event> {
    let mut events = Vec::new();
    for _ in 0..1000 {
        match owner.poll_next().unwrap() {
            ServicePoll::Event(event) => events.push(event),
            ServicePoll::Progress => {}
            ServicePoll::Pending => return events,
        }
    }
    panic!("bounded fixture did not reach pending");
}

pub(super) fn finish_recovery(owner: &mut Owner, wake: &mpsc::Receiver<()>) -> Vec<Event> {
    let mut events = Vec::new();
    loop {
        events.extend(drain(owner));
        if owner.state() == ServiceState::Ready {
            return events;
        }
        wake.recv_timeout(Duration::from_secs(5))
            .expect("no periodic poll fallback");
    }
}

#[test]
fn coalesced_wakes_recover_more_commands_than_credits_and_publish_readiness() {
    let mut store = Store::default();
    let (started, release) = gate(&mut store);
    let (mut owner, wake) = notified(log_with_commands(64), store, 2);
    let mut release = release;
    assert!(drain(&mut owner).is_empty());
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(wake.try_recv().is_err());
    assert_eq!(owner.dispatched_through(), LogIndex(3));
    // Completion happens after Pending but before waiting: the token survives.
    release.now();
    wake.recv_timeout(Duration::from_secs(5)).unwrap();
    let events = finish_recovery(&mut owner, &wake);
    let positions: Vec<_> = events
        .into_iter()
        .flat_map(|event| match event {
            ServiceEvent::Applied(done) => {
                done.entries().iter().map(|e| e.last).collect::<Vec<_>>()
            }
            other => panic!("unexpected recovery event: {other:?}"),
        })
        .collect();
    assert_eq!(positions, (2..=65).map(LogIndex).collect::<Vec<_>>());
    assert_eq!(owner.durable_through(), Some(LogIndex(65)));
    assert!(matches!(owner.poll_next().unwrap(), ServicePoll::Pending));
    owner.begin_shutdown();
    assert_eq!(owner.finish_shutdown().unwrap().floor, LogIndex(65));
}

#[test]
fn successful_dispatch_reports_progress_even_while_its_worker_is_blocked() {
    let mut store = Store::default();
    let (started, release) = gate(&mut store);
    let (mut owner, wake) = notified(log_with_commands(3), store, 1);
    let mut release = release;
    assert!(matches!(owner.poll_next().unwrap(), ServicePoll::Progress));
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(matches!(owner.poll_next().unwrap(), ServicePoll::Pending));
    assert_eq!(owner.dispatched_through(), LogIndex(2));
    release.now();
    assert_eq!(finish_recovery(&mut owner, &wake).len(), 3);
}

#[test]
fn application_failure_or_panic_wakes_and_terminally_fences_the_owner() {
    for panic in [false, true] {
        let mut store = Store {
            fail: !panic,
            panic,
            ..Store::default()
        };
        let (started, release) = gate(&mut store);
        let (mut owner, wake) = notified(log_with_commands(2), store, 1);
        let mut release = release;
        assert!(drain(&mut owner).is_empty());
        started.recv_timeout(Duration::from_secs(5)).unwrap();
        release.now();
        wake.recv_timeout(Duration::from_secs(5)).unwrap();
        if panic {
            assert!(matches!(
                owner.poll_next(),
                Err(ServiceError::ApplicationStopped)
            ));
        } else {
            assert!(matches!(
                owner.poll_next().unwrap(),
                ServicePoll::Event(ServiceEvent::ApplicationFailed(_))
            ));
        }
        assert_eq!(owner.state(), ServiceState::Failed);
        assert!(owner.try_step(Input::Tick.into()).is_err());
        assert!(matches!(owner.poll_next(), Err(ServiceError::Failed)));
    }
}
