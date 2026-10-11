//! Views cannot observe application work before validation or after failure.

use super::*;
use crate::application::{ApplicationReadError, ApplicationReadRejection};
use std::{cell::Cell, panic::AssertUnwindSafe, rc::Rc};

#[test]
fn floor_refusal_returns_the_owned_query_without_invoking_it() {
    let (mut worker, _, _) = worker(3, Mode::Normal, ApplicationWorkerOptions::new(), false);
    // Neither this callback nor Store is Sync; the callback need not be Send.
    let called = Rc::new(Cell::new(false));
    let observed = Rc::clone(&called);
    let input = String::from("owned query input");
    let query = move |store: &Store| {
        called.set(true);
        (input, store.applied)
    };
    let refused = worker.try_read(LogIndex(4), query).unwrap_err();
    assert!(!observed.get());
    assert_eq!(
        refused.rejection(),
        ApplicationReadRejection::Behind {
            required: LogIndex(4),
            durable_through: LogIndex(3),
        }
    );
    let read = worker.try_read(LogIndex(2), refused.into_query()).unwrap();
    assert!(observed.get());
    assert_eq!(read.durable_through(), LogIndex(3));
    assert_eq!(read.output().1, LogIndex(3));
    assert_eq!(read.into_output().0, "owned query input");
    assert_eq!(worker.shutdown_into_store().unwrap().applied, LogIndex(3));
}

#[test]
fn read_never_waits_for_a_blocked_apply_or_invokes_its_query() {
    let (mut worker, gate, _) = worker(0, Mode::Normal, ApplicationWorkerOptions::new(), true);
    let (entered, release) = gate.unwrap();
    worker.try_submit(vec![Entry::new(1)]).unwrap();
    entered.recv_timeout(Duration::from_secs(1)).unwrap();
    // Run on another thread so a mistakenly blocking implementation fails
    // with a bounded timeout and still releases the joined application worker.
    let (read_finished, observed) = mpsc::sync_channel(1);
    let reader = std::thread::spawn(move || {
        let result = worker.try_read(LogIndex::ZERO, |store| store.applied);
        read_finished
            .send(result.as_ref().err().map(ApplicationReadError::rejection))
            .unwrap();
        (worker, result)
    });
    let result = observed.recv_timeout(Duration::from_secs(1));
    release.send(()).unwrap();
    let (mut worker, read) = reader.join().unwrap();
    assert_eq!(result.unwrap(), Some(ApplicationReadRejection::Busy));
    let query = read.unwrap_err().into_query();
    worker.complete().unwrap();
    let read = worker.try_read(LogIndex(1), query).unwrap();
    assert_eq!(read.durable_through(), LogIndex(1));
    assert_eq!(read.into_output(), LogIndex(1));
    worker.shutdown().unwrap();
}

#[test]
fn notification_publishes_the_view_before_completion_credits_are_released() {
    let (wake, woken) = mpsc::sync_channel(1);
    let (release, resume) = mpsc::sync_channel::<()>(1);
    let (mut worker, _, _) = worker_with_wake(
        0,
        Mode::Normal,
        ApplicationWorkerOptions::new(),
        false,
        move || {
            let _ = wake.try_send(());
            let _ = resume.recv_timeout(Duration::from_secs(5));
        },
    );
    worker.try_submit(vec![Entry::new(1)]).unwrap();
    woken.recv_timeout(Duration::from_secs(1)).unwrap();
    let read = worker.try_read(LogIndex(1), |store| store.applied);
    // Release the hook on failure too; a test assertion must not strand Drop.
    drop(release);
    let read = read.unwrap();
    assert_eq!(read.durable_through(), LogIndex(1));
    assert_eq!(read.into_output(), LogIndex(1));
    assert!(worker.is_busy(), "unconsumed completion still owns credit");
    worker.complete().unwrap();
    worker.shutdown().unwrap();
}

#[test]
fn failed_or_unvalidated_state_is_unreadable_before_consuming_the_failure() {
    for mode in [Mode::Fail, Mode::WrongCount, Mode::WrongFloor, Mode::Panic] {
        let (wake, woken) = mpsc::sync_channel(1);
        let (mut worker, _, _) =
            worker_with_wake(0, mode, ApplicationWorkerOptions::new(), false, move || {
                let _ = wake.try_send(());
            });
        worker.try_submit(vec![Entry::new(1)]).unwrap();
        woken.recv_timeout(Duration::from_secs(1)).unwrap();
        let read = worker.try_read(LogIndex::ZERO, |_| panic!("invalid state escaped"));
        assert_eq!(
            read.unwrap_err().rejection(),
            ApplicationReadRejection::Stopped
        );
        assert_eq!(worker.durable_through(), LogIndex::ZERO);
        let _ = worker.complete();
        let _ = worker.shutdown();
    }
}

#[test]
fn a_caught_read_panic_stops_an_idle_worker_and_wakes_its_owner() {
    let (wake, woken) = mpsc::sync_channel(1);
    let (mut worker, _, _) = worker_with_wake(
        3,
        Mode::Normal,
        ApplicationWorkerOptions::new(),
        false,
        move || {
            let _ = wake.try_send(());
        },
    );
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let _ = worker.try_read(LogIndex(3), |_| panic!("query panic"));
    }));
    assert!(result.is_err());
    assert!(!worker.is_accepting());
    assert_eq!(
        worker
            .try_submit(vec![Entry::new(4)])
            .unwrap_err()
            .rejection(),
        ApplicationSubmitRejection::Stopped
    );
    assert_eq!(
        worker
            .try_read(LogIndex::ZERO, |_| ())
            .unwrap_err()
            .rejection(),
        ApplicationReadRejection::Stopped
    );
    woken.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(worker.complete().is_err());
    assert!(matches!(
        worker.shutdown_into_store(),
        Err(ApplicationWorkerShutdownError::Panicked)
    ));
}

#[test]
fn a_read_panic_prevents_already_queued_work_from_reusing_the_poisoned_store() {
    let (wake, woken) = mpsc::sync_channel(1);
    let (release, resume) = mpsc::sync_channel::<()>(1);
    let (mut worker, _, batches) = worker_with_wake(
        0,
        Mode::Normal,
        ApplicationWorkerOptions::new(),
        false,
        move || {
            let _ = wake.try_send(());
            let _ = resume.recv_timeout(Duration::from_secs(5));
        },
    );
    worker.try_submit(vec![Entry::new(1)]).unwrap();
    woken.recv_timeout(Duration::from_secs(1)).unwrap();
    worker.complete().unwrap();
    worker.try_submit(vec![Entry::new(2)]).unwrap();
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let _ = worker.try_read(LogIndex(1), |_| panic!("query panic"));
    }));
    drop(release);
    assert!(result.is_err());
    assert!(worker.complete().is_err());
    assert_eq!(*batches.lock().unwrap(), vec![vec![LogIndex(1)]]);
    assert_eq!(worker.durable_through(), LogIndex(1));
    assert!(!worker.is_busy());
    assert_eq!(
        worker.shutdown(),
        Err(ApplicationWorkerShutdownError::Panicked)
    );
}

#[test]
fn both_shutdown_forms_discard_the_read_view() {
    for return_store in [false, true] {
        let (mut worker, _, _) = worker(3, Mode::Normal, ApplicationWorkerOptions::new(), false);
        if return_store {
            assert_eq!(worker.shutdown_into_store().unwrap().applied, LogIndex(3));
        } else {
            worker.shutdown().unwrap();
        }
        let refused = worker.try_read(LogIndex::ZERO, |_| panic!("read after shutdown"));
        assert_eq!(
            refused.unwrap_err().rejection(),
            ApplicationReadRejection::Stopped
        );
        worker.shutdown().unwrap();
    }
}
