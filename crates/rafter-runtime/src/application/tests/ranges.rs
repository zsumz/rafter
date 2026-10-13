//! Range admission preserves bounded credits, ownership, and durable completion.

use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
struct Span {
    first: LogIndex,
    last: LogIndex,
}

impl Span {
    const fn new(first: u64, last: u64) -> Self {
        Self {
            first: LogIndex(first),
            last: LogIndex(last),
        }
    }
}

impl ApplicationEntry for Span {
    fn first_log_index(&self) -> LogIndex {
        self.first
    }
    fn log_index(&self) -> LogIndex {
        self.last
    }
    fn retained_bytes(&self) -> usize {
        1
    }
}

#[test]
fn a_large_protocol_prefix_uses_one_credit_and_waits_for_durability() {
    let limits = ApplicationWorkerOptions::new().with_limits(1, 1, 1, 1);
    let (mut worker, gate, batches) = worker_with_wake_for(5, Mode::Normal, limits, true, || {});
    let (entered, release) = gate.unwrap();
    let span = Span::new(6, u64::MAX - 1);
    worker.try_submit(vec![span.clone()]).unwrap();
    entered.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(worker.applying_through(), span.last);
    assert_eq!(worker.durable_through(), LogIndex(5));
    assert!(worker.try_complete().unwrap().is_none());
    assert_eq!(worker.available(), (0, 0));
    let next = Span::new(u64::MAX, u64::MAX);
    let refused = worker.try_submit(vec![next.clone()]).unwrap_err();
    assert!(matches!(
        refused.rejection(),
        ApplicationSubmitRejection::Full { .. }
    ));
    assert_eq!(refused.into_entries(), vec![next.clone()]);

    release.send(()).unwrap();
    let ApplicationEvent::Applied(completion) = worker.complete().unwrap() else {
        panic!("the released range must become durable");
    };
    assert_eq!(completion.entries(), &[span]);
    assert_eq!(completion.outcomes(), &[LogIndex(u64::MAX - 1)]);
    assert_eq!(worker.durable_through(), LogIndex(u64::MAX - 1));
    assert_eq!(worker.available(), (1, 1));
    worker.try_submit(vec![next]).unwrap();
    assert!(matches!(
        worker.complete().unwrap(),
        ApplicationEvent::Applied(_)
    ));
    assert_eq!(worker.durable_through(), LogIndex(u64::MAX));
    let refused = worker.try_submit(vec![Span::new(0, 0)]).unwrap_err();
    assert_eq!(
        refused.rejection(),
        ApplicationSubmitRejection::IndexExhausted {
            after: LogIndex(u64::MAX),
        }
    );
    assert_eq!(
        *batches.lock().unwrap(),
        vec![vec![LogIndex(u64::MAX - 1)], vec![LogIndex(u64::MAX)]]
    );
    worker.shutdown().unwrap();
}

#[test]
fn reversed_ranges_gaps_and_overlaps_return_all_items_without_reserving_indexes() {
    let (mut worker, _, batches) = worker_with_wake_for(
        5,
        Mode::Normal,
        ApplicationWorkerOptions::new(),
        false,
        || {},
    );
    let cases = [
        (
            vec![Span::new(6, 5)],
            ApplicationSubmitRejection::InvalidRange {
                first: LogIndex(6),
                last: LogIndex(5),
            },
        ),
        (
            vec![Span::new(7, 9)],
            ApplicationSubmitRejection::NonContiguous {
                expected: LogIndex(6),
                actual: LogIndex(7),
            },
        ),
        (
            vec![Span::new(6, 9), Span::new(10, 8)],
            ApplicationSubmitRejection::InvalidRange {
                first: LogIndex(10),
                last: LogIndex(8),
            },
        ),
        (
            vec![Span::new(6, 9), Span::new(9, 15)],
            ApplicationSubmitRejection::NonContiguous {
                expected: LogIndex(10),
                actual: LogIndex(9),
            },
        ),
        (
            vec![Span::new(6, 9), Span::new(11, 15)],
            ApplicationSubmitRejection::NonContiguous {
                expected: LogIndex(10),
                actual: LogIndex(11),
            },
        ),
        (
            vec![Span::new(6, u64::MAX), Span::new(u64::MAX, u64::MAX)],
            ApplicationSubmitRejection::IndexExhausted {
                after: LogIndex(u64::MAX),
            },
        ),
    ];
    let credits = worker.available();
    for (entries, rejection) in cases {
        let refused = worker.try_submit(entries.clone()).unwrap_err();
        assert_eq!(refused.rejection(), rejection);
        assert_eq!(refused.into_entries(), entries);
        assert_eq!(worker.available(), credits);
        assert_eq!(worker.durable_through(), LogIndex(5));
        assert!(!worker.is_busy());
    }
    assert!(batches.lock().unwrap().is_empty());
    worker
        .try_submit(vec![Span::new(6, 9), Span::new(10, 15)])
        .unwrap();
    let ApplicationEvent::Applied(completion) = worker.complete().unwrap() else {
        panic!("valid contiguous ranges must complete");
    };
    assert_eq!(completion.outcomes(), &[LogIndex(9), LogIndex(15)]);
    assert_eq!(worker.durable_through(), LogIndex(15));
    worker.shutdown().unwrap();
}

#[test]
fn range_failure_returns_attempted_and_queued_ranges_without_an_acknowledgement() {
    let limits = ApplicationWorkerOptions::new().with_limits(2, 2, 1, 1);
    let (mut worker, gate, _) = worker_with_wake_for(0, Mode::Fail, limits, true, || {});
    let (entered, release) = gate.unwrap();
    let attempted = Span::new(1, 500);
    let queued = Span::new(501, u64::MAX);
    worker.try_submit(vec![attempted.clone()]).unwrap();
    entered.recv_timeout(Duration::from_secs(1)).unwrap();
    worker.try_submit(vec![queued.clone()]).unwrap();
    release.send(()).unwrap();
    let ApplicationEvent::Failed(failure) = worker.complete().unwrap() else {
        panic!("failed ranges must never be acknowledged");
    };
    assert_eq!(failure.attempted(), &[attempted]);
    assert_eq!(failure.unattempted(), &[queued]);
    assert_eq!(worker.durable_through(), LogIndex::ZERO);
    assert_eq!(worker.available(), (2, 2));
    assert!(!worker.is_accepting());
    worker.shutdown().unwrap();
}

#[test]
fn a_range_still_requires_one_outcome_and_its_exact_final_durable_floor() {
    for mode in [Mode::WrongCount, Mode::WrongFloor] {
        let (mut worker, _, _) =
            worker_with_wake_for(0, mode, ApplicationWorkerOptions::new(), false, || {});
        worker.try_submit(vec![Span::new(1, 10_000)]).unwrap();
        let ApplicationEvent::Failed(failure) = worker.complete().unwrap() else {
            panic!("an invalid success must fail the worker");
        };
        match mode {
            Mode::WrongCount => assert!(matches!(
                failure.kind(),
                ApplicationFailureKind::OutcomeCount {
                    expected: 1,
                    actual: 0
                }
            )),
            Mode::WrongFloor => assert!(matches!(
                failure.kind(),
                ApplicationFailureKind::DurableFloor {
                    expected: LogIndex(10_000),
                    actual: LogIndex(0)
                }
            )),
            _ => unreachable!(),
        }
        assert_eq!(worker.durable_through(), LogIndex::ZERO);
        worker.shutdown().unwrap();
    }
}

#[test]
fn range_admission_still_enforces_item_and_byte_limits() {
    let limits = ApplicationWorkerOptions::new().with_limits(2, 2, 1, 1);
    let (mut worker, _, _) = worker_with_wake_for(0, Mode::Normal, limits, false, || {});
    let entries = vec![Span::new(1, 10_000), Span::new(10_001, 20_000)];
    let refused = worker.try_submit(entries.clone()).unwrap_err();
    assert_eq!(
        refused.rejection(),
        ApplicationSubmitRejection::BatchTooLarge {
            entries: 2,
            bytes: 2,
            max_entries: 1,
            max_bytes: 1,
        }
    );
    assert_eq!(refused.into_entries(), entries);
    assert_eq!(worker.durable_through(), LogIndex::ZERO);
    worker.shutdown().unwrap();
}
