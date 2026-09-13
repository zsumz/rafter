//! Ownership, durability, ordering, and bounds for the public application worker.

use super::{
    ApplicationEntry, ApplicationEvent, ApplicationFailureKind, ApplicationSubmitRejection,
    ApplicationWorker, ApplicationWorkerOptions, ApplicationWorkerShutdownError,
    DurableApplication,
};
use rafter::LogIndex;
use std::{
    error::Error,
    fmt,
    sync::{mpsc, Arc, Mutex},
    time::Duration,
};

#[derive(Clone, Debug, Eq, PartialEq)]
struct Entry {
    index: LogIndex,
    retained_bytes: usize,
    batch_bytes: usize,
}

impl Entry {
    const fn new(index: u64) -> Self {
        Self {
            index: LogIndex(index),
            retained_bytes: 1,
            batch_bytes: 1,
        }
    }
}

impl ApplicationEntry for Entry {
    fn log_index(&self) -> LogIndex {
        self.index
    }

    fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    fn batch_bytes(&self) -> usize {
        self.batch_bytes
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TestError;

impl fmt::Display for TestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("injected application failure")
    }
}

impl Error for TestError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
    Normal,
    Fail,
    Panic,
    WrongCount,
    WrongFloor,
}

struct Store {
    applied: LogIndex,
    mode: Mode,
    pause: Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>,
    batches: Arc<Mutex<Vec<Vec<LogIndex>>>>,
}

type PauseGate = Option<(mpsc::Receiver<()>, mpsc::SyncSender<()>)>;
type BatchLog = Arc<Mutex<Vec<Vec<LogIndex>>>>;
type TestWorker = ApplicationWorker<Entry, Store>;

impl DurableApplication<Entry> for Store {
    type Outcome = LogIndex;
    type Error = TestError;

    fn applied_through(&self) -> LogIndex {
        self.applied
    }

    fn apply(&mut self, entries: &[Entry]) -> Result<Vec<Self::Outcome>, Self::Error> {
        if let Some((entered, resume)) = self.pause.take() {
            entered.send(()).expect("test observes apply entry");
            resume.recv().expect("test releases apply");
        }
        self.batches
            .lock()
            .expect("batch observations lock")
            .push(entries.iter().map(ApplicationEntry::log_index).collect());
        if self.mode == Mode::Fail {
            return Err(TestError);
        }
        assert_ne!(self.mode, Mode::Panic, "injected application panic");
        let outcomes: Vec<_> = entries.iter().map(ApplicationEntry::log_index).collect();
        if self.mode != Mode::WrongFloor {
            self.applied = *outcomes.last().expect("worker batches are nonempty");
        }
        if self.mode == Mode::WrongCount {
            return Ok(Vec::new());
        }
        Ok(outcomes)
    }
}

fn worker(
    applied: u64,
    mode: Mode,
    limits: ApplicationWorkerOptions,
    pause: bool,
) -> (TestWorker, PauseGate, BatchLog) {
    let (entered, entering) = mpsc::sync_channel(0);
    let (release, resume) = mpsc::sync_channel(0);
    let batches = Arc::new(Mutex::new(Vec::new()));
    let store = Store {
        applied: LogIndex(applied),
        mode,
        pause: pause.then_some((entered, resume)),
        batches: Arc::clone(&batches),
    };
    let worker = ApplicationWorker::start(store, limits, || {}).expect("worker starts");
    (worker, pause.then_some((entering, release)), batches)
}

#[test]
fn delayed_store_cannot_complete_or_advance_the_durable_floor_early() {
    let (mut worker, gate, _) = worker(0, Mode::Normal, ApplicationWorkerOptions::new(), true);
    let (entered, release) = gate.expect("pause gate");
    worker.try_submit(vec![Entry::new(1)]).unwrap();
    entered.recv_timeout(Duration::from_secs(1)).unwrap();

    assert_eq!(worker.applying_through(), LogIndex(1));
    assert_eq!(worker.durable_through(), LogIndex::ZERO);
    assert!(worker.try_complete().unwrap().is_none());

    release.send(()).unwrap();
    let ApplicationEvent::Applied(completion) = worker.complete().unwrap() else {
        panic!("released durable application must complete")
    };
    assert_eq!(completion.entries(), &[Entry::new(1)]);
    assert_eq!(completion.outcomes(), &[LogIndex(1)]);
    assert_eq!(worker.durable_through(), LogIndex(1));
    worker.shutdown().unwrap();
}

#[test]
fn submissions_are_contiguous_from_the_recovered_application_floor() {
    let (mut worker, _, _) = worker(5, Mode::Normal, ApplicationWorkerOptions::new(), false);
    let error = worker.try_submit(vec![Entry::new(7)]).unwrap_err();
    assert_eq!(
        error.rejection(),
        ApplicationSubmitRejection::NonContiguous {
            expected: LogIndex(6),
            actual: LogIndex(7),
        }
    );
    assert_eq!(error.into_entries(), vec![Entry::new(7)]);
    worker.shutdown().unwrap();
}

#[test]
fn exhausted_log_index_is_refused_without_wrapping() {
    let (mut worker, _, _) = worker(
        u64::MAX,
        Mode::Normal,
        ApplicationWorkerOptions::new(),
        false,
    );
    let error = worker.try_submit(vec![Entry::new(u64::MAX)]).unwrap_err();
    assert_eq!(
        error.rejection(),
        ApplicationSubmitRejection::IndexExhausted {
            after: LogIndex(u64::MAX),
        }
    );
    worker.shutdown().unwrap();
}

#[test]
fn credits_cover_queued_executing_and_unconsumed_entries() {
    let limits = ApplicationWorkerOptions::new().with_limits(2, 2, 1, 1);
    let (mut worker, gate, _) = worker(0, Mode::Normal, limits, true);
    let (entered, release) = gate.expect("pause gate");
    worker.try_submit(vec![Entry::new(1)]).unwrap();
    entered.recv_timeout(Duration::from_secs(1)).unwrap();
    worker.try_submit(vec![Entry::new(2)]).unwrap();
    let error = worker.try_submit(vec![Entry::new(3)]).unwrap_err();
    assert!(matches!(
        error.rejection(),
        ApplicationSubmitRejection::Full {
            available_entries: 0,
            available_bytes: 0,
        }
    ));
    assert_eq!(worker.available(), (0, 0));

    release.send(()).unwrap();
    while worker.durable_through() < LogIndex(2) {
        std::thread::yield_now();
    }
    assert_eq!(worker.available(), (0, 0));
    for _ in 0..2 {
        assert!(matches!(
            worker.complete().unwrap(),
            ApplicationEvent::Applied(_)
        ));
    }
    assert_eq!(worker.available(), (2, 2));
    worker.shutdown().unwrap();
}

#[test]
fn store_failure_stops_admission_and_returns_all_owned_work() {
    let limits = ApplicationWorkerOptions::new().with_limits(4, 4, 1, 1);
    let (mut worker, gate, _) = worker(0, Mode::Fail, limits, true);
    let (entered, release) = gate.expect("pause gate");
    worker.try_submit(vec![Entry::new(1)]).unwrap();
    entered.recv_timeout(Duration::from_secs(1)).unwrap();
    worker.try_submit(vec![Entry::new(2)]).unwrap();
    release.send(()).unwrap();

    let ApplicationEvent::Failed(failure) = worker.complete().unwrap() else {
        panic!("store error must fail the worker")
    };
    assert_eq!(failure.attempted(), &[Entry::new(1)]);
    assert_eq!(failure.unattempted(), &[Entry::new(2)]);
    assert!(matches!(
        failure.kind(),
        ApplicationFailureKind::Store(TestError)
    ));
    assert!(!worker.is_accepting());
    let error = worker.try_submit(vec![Entry::new(3)]).unwrap_err();
    assert_eq!(error.rejection(), ApplicationSubmitRejection::Stopped);
    let error = worker.try_submit(Vec::new()).unwrap_err();
    assert_eq!(error.rejection(), ApplicationSubmitRejection::Stopped);
    worker.shutdown().unwrap();
}

#[test]
fn application_panic_closes_admission_and_is_observable_at_shutdown() {
    let (mut worker, _, _) = worker(0, Mode::Panic, ApplicationWorkerOptions::new(), false);
    worker.try_submit(vec![Entry::new(1)]).unwrap();
    assert!(matches!(
        worker.complete(),
        Err(super::ApplicationWorkerStopped)
    ));
    assert!(!worker.is_accepting());
    assert!(!worker.is_busy());
    assert_eq!(
        worker.shutdown(),
        Err(ApplicationWorkerShutdownError::Panicked)
    );
}

#[test]
fn successful_store_must_return_one_outcome_per_entry() {
    let (mut worker, _, _) = worker(0, Mode::WrongCount, ApplicationWorkerOptions::new(), false);
    worker.try_submit(vec![Entry::new(1)]).unwrap();
    let ApplicationEvent::Failed(failure) = worker.complete().unwrap() else {
        panic!("outcome mismatch must fail the worker")
    };
    assert!(matches!(
        failure.kind(),
        ApplicationFailureKind::OutcomeCount {
            expected: 1,
            actual: 0,
        }
    ));
    worker.shutdown().unwrap();
}

#[test]
fn successful_store_must_publish_the_final_applied_floor() {
    let (mut worker, _, _) = worker(0, Mode::WrongFloor, ApplicationWorkerOptions::new(), false);
    worker.try_submit(vec![Entry::new(1)]).unwrap();
    let ApplicationEvent::Failed(failure) = worker.complete().unwrap() else {
        panic!("durable floor mismatch must fail the worker")
    };
    assert!(matches!(
        failure.kind(),
        ApplicationFailureKind::DurableFloor {
            expected: LogIndex(1),
            actual: LogIndex::ZERO,
        }
    ));
    worker.shutdown().unwrap();
}

#[test]
fn oversized_batch_is_refused_with_ownership_intact() {
    let limits = ApplicationWorkerOptions::new().with_limits(4, 16, 2, 4);
    let (mut worker, _, _) = worker(0, Mode::Normal, limits, false);
    let entries = vec![
        Entry {
            index: LogIndex(1),
            retained_bytes: 1,
            batch_bytes: 3,
        },
        Entry {
            index: LogIndex(2),
            retained_bytes: 1,
            batch_bytes: 3,
        },
    ];
    let error = worker.try_submit(entries.clone()).unwrap_err();
    assert!(matches!(
        error.rejection(),
        ApplicationSubmitRejection::BatchTooLarge {
            entries: 2,
            bytes: 6,
            max_entries: 2,
            max_bytes: 4,
        }
    ));
    assert_eq!(error.into_entries(), entries);
    worker.shutdown().unwrap();
}

#[test]
fn batch_work_bytes_are_independent_of_retained_queue_bytes() {
    let limits = ApplicationWorkerOptions::new().with_limits(2, 2, 2, 20);
    let (mut worker, _, batches) = worker(0, Mode::Normal, limits, false);
    worker
        .try_submit(vec![
            Entry {
                index: LogIndex(1),
                retained_bytes: 1,
                batch_bytes: 10,
            },
            Entry {
                index: LogIndex(2),
                retained_bytes: 1,
                batch_bytes: 10,
            },
        ])
        .unwrap();
    assert!(matches!(
        worker.complete().unwrap(),
        ApplicationEvent::Applied(_)
    ));
    assert_eq!(
        *batches.lock().unwrap(),
        vec![vec![LogIndex(1), LogIndex(2)]]
    );
    worker.shutdown().unwrap();
}

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
fn invalid_limits_fail_before_the_thread_starts() {
    let batches = Arc::new(Mutex::new(Vec::new()));
    let store = Store {
        applied: LogIndex::ZERO,
        mode: Mode::Normal,
        pause: None,
        batches,
    };
    let error = ApplicationWorker::start(
        store,
        ApplicationWorkerOptions::new().with_limits(1, 1, 2, 1),
        || {},
    )
    .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
}
