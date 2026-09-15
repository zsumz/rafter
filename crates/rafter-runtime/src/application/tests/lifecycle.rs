//! Explicit worker shutdown and durable-store ownership tests.

use super::*;

#[test]
fn explicit_shutdown_refuses_unconsumed_completion() {
    let (mut worker, _, _) = worker(0, Mode::Normal, ApplicationWorkerOptions::new(), false);
    worker.try_submit(vec![Entry::new(1)]).unwrap();
    while worker.durable_through() < LogIndex(1) {
        std::thread::yield_now();
    }
    assert_eq!(worker.shutdown(), Err(ApplicationWorkerShutdownError::Busy));
    assert!(matches!(
        worker.complete().unwrap(),
        ApplicationEvent::Applied(_)
    ));
    worker.shutdown().unwrap();
}

#[test]
fn idle_shutdown_returns_the_durable_store() {
    let (mut worker, _, _) = worker(3, Mode::Normal, ApplicationWorkerOptions::new(), false);
    worker.try_submit(vec![Entry::new(4)]).unwrap();
    assert!(matches!(
        worker.complete().unwrap(),
        ApplicationEvent::Applied(_)
    ));

    let store = worker.shutdown_into_store().unwrap();
    assert_eq!(store.applied, LogIndex(4));
    assert!(matches!(
        worker.shutdown_into_store(),
        Err(ApplicationWorkerShutdownError::StoreUnavailable)
    ));
}

#[test]
fn store_return_refuses_while_a_completion_owns_credit() {
    let (mut worker, _, _) = worker(0, Mode::Normal, ApplicationWorkerOptions::new(), false);
    worker.try_submit(vec![Entry::new(1)]).unwrap();
    while worker.durable_through() < LogIndex(1) {
        std::thread::yield_now();
    }
    assert!(matches!(
        worker.shutdown_into_store(),
        Err(ApplicationWorkerShutdownError::Busy)
    ));
    assert!(matches!(
        worker.complete().unwrap(),
        ApplicationEvent::Applied(_)
    ));
    assert_eq!(worker.shutdown_into_store().unwrap().applied, LogIndex(1));
}

#[test]
fn application_panic_closes_admission_and_is_observable_at_shutdown() {
    let (mut worker, _, _) = worker(0, Mode::Panic, ApplicationWorkerOptions::new(), false);
    worker.try_submit(vec![Entry::new(1)]).unwrap();
    assert!(matches!(
        worker.complete(),
        Err(super::super::ApplicationWorkerStopped)
    ));
    assert!(!worker.is_accepting());
    assert!(!worker.is_busy());
    assert_eq!(
        worker.shutdown(),
        Err(ApplicationWorkerShutdownError::Panicked)
    );
}

#[test]
fn application_panic_wakes_a_parked_owner_after_publishing_termination() {
    let (wake, woken) = mpsc::sync_channel(1);
    let (mut worker, gate, _) = worker_with_wake(
        0,
        Mode::Panic,
        ApplicationWorkerOptions::new(),
        true,
        move || {
            let _ = wake.try_send(());
        },
    );
    let (entered, release) = gate.expect("pause gate");
    worker.try_submit(vec![Entry::new(1)]).unwrap();
    entered.recv_timeout(Duration::from_secs(1)).unwrap();

    assert!(worker.try_complete().unwrap().is_none());
    assert_eq!(worker.applying_through(), LogIndex(1));
    release.send(()).unwrap();
    woken.recv_timeout(Duration::from_secs(1)).unwrap();

    assert_eq!(worker.applying_through(), LogIndex::ZERO);
    assert!(!worker.is_accepting());
    assert!(matches!(
        worker.try_complete(),
        Err(super::super::ApplicationWorkerStopped)
    ));
    assert!(!worker.is_busy());
    assert_eq!(
        worker.shutdown(),
        Err(ApplicationWorkerShutdownError::Panicked)
    );
}
