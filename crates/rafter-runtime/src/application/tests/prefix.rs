//! Prefix dispatch respects credits and preserves every unsubmitted item.

use super::*;
use std::collections::VecDeque;

#[test]
fn full_worker_preserves_the_remainder_until_completion_is_consumed() {
    let limits = ApplicationWorkerOptions::new().with_limits(4, 4, 2, 2);
    let (mut worker, gate, batches) = worker(0, Mode::Normal, limits, true);
    let (entered, release) = gate.unwrap();
    let mut queued: VecDeque<_> = (1..=5).map(Entry::new).collect();
    assert_eq!(worker.try_submit_prefix(&mut queued), Ok(2));
    entered.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(worker.try_submit_prefix(&mut queued), Ok(2));
    assert_eq!(queued, VecDeque::from([Entry::new(5)]));
    assert_eq!(
        worker.try_submit_prefix(&mut queued),
        Err(ApplicationSubmitRejection::Full {
            available_entries: 0,
            available_bytes: 0,
        })
    );
    assert_eq!(queued, VecDeque::from([Entry::new(5)]));
    assert_eq!(worker.durable_through(), LogIndex::ZERO);
    release.send(()).unwrap();
    for _ in 0..2 {
        assert!(matches!(
            worker.complete().unwrap(),
            ApplicationEvent::Applied(_)
        ));
    }
    assert_eq!(worker.try_submit_prefix(&mut queued), Ok(1));
    assert!(queued.is_empty());
    assert_eq!(worker.try_submit_prefix(&mut queued), Ok(0));
    assert!(matches!(
        worker.complete().unwrap(),
        ApplicationEvent::Applied(_)
    ));
    assert_eq!(worker.durable_through(), LogIndex(5));
    assert_eq!(
        *batches.lock().unwrap(),
        vec![
            vec![LogIndex(1), LogIndex(2)],
            vec![LogIndex(3), LogIndex(4)],
            vec![LogIndex(5)]
        ]
    );
    worker.shutdown().unwrap();
}

#[test]
fn work_and_retained_byte_limits_choose_distinct_prefixes() {
    let limits = ApplicationWorkerOptions::new().with_limits(4, 10, 3, 4);
    let (mut worker, gate, _) = worker(0, Mode::Normal, limits, true);
    let (entered, release) = gate.unwrap();
    let mut queued = VecDeque::from([
        Entry {
            index: LogIndex(1),
            retained_bytes: 2,
            batch_bytes: 3,
        },
        Entry {
            index: LogIndex(2),
            retained_bytes: 7,
            batch_bytes: 1,
        },
        Entry {
            index: LogIndex(3),
            retained_bytes: 2,
            batch_bytes: 1,
        },
    ]);
    assert_eq!(worker.try_submit_prefix(&mut queued), Ok(2));
    entered.recv_timeout(Duration::from_secs(1)).unwrap();
    let remainder = queued.clone();
    assert_eq!(
        worker.try_submit_prefix(&mut queued),
        Err(ApplicationSubmitRejection::Full {
            available_entries: 2,
            available_bytes: 1,
        })
    );
    assert_eq!(queued, remainder);
    release.send(()).unwrap();
    assert!(matches!(
        worker.complete().unwrap(),
        ApplicationEvent::Applied(_)
    ));
    assert_eq!(worker.try_submit_prefix(&mut queued), Ok(1));
    assert!(matches!(
        worker.complete().unwrap(),
        ApplicationEvent::Applied(_)
    ));
    assert_eq!(worker.durable_through(), LogIndex(3));
    worker.shutdown().unwrap();
}

#[test]
fn invalid_selected_prefix_and_oversized_first_item_leave_the_queue_unchanged() {
    let limits = ApplicationWorkerOptions::new().with_limits(3, 4, 2, 4);
    let (mut worker, _, _) = worker(0, Mode::Normal, limits, false);
    let mut gap = VecDeque::from([Entry::new(1), Entry::new(3), Entry::new(4)]);
    let original = gap.clone();
    assert_eq!(
        worker.try_submit_prefix(&mut gap),
        Err(ApplicationSubmitRejection::NonContiguous {
            expected: LogIndex(2),
            actual: LogIndex(3),
        })
    );
    assert_eq!(gap, original);
    let mut oversized = VecDeque::from([
        Entry {
            index: LogIndex(1),
            retained_bytes: 1,
            batch_bytes: 5,
        },
        Entry::new(2),
    ]);
    let original = oversized.clone();
    assert_eq!(
        worker.try_submit_prefix(&mut oversized),
        Err(ApplicationSubmitRejection::BatchTooLarge {
            entries: 1,
            bytes: 5,
            max_entries: 2,
            max_bytes: 4,
        })
    );
    assert_eq!(oversized, original);
    assert_eq!(worker.available(), (3, 4));
    assert_eq!(worker.durable_through(), LogIndex::ZERO);
    worker.shutdown().unwrap();
    assert_eq!(
        worker.try_submit_prefix(&mut gap),
        Err(ApplicationSubmitRejection::Stopped)
    );
    assert_eq!(
        gap,
        VecDeque::from([Entry::new(1), Entry::new(3), Entry::new(4)])
    );
}

#[test]
fn a_failure_owns_only_the_submitted_prefix_and_leaves_the_tail_with_the_owner() {
    let limits = ApplicationWorkerOptions::new().with_limits(2, 2, 1, 1);
    let (mut worker, gate, _) = worker(0, Mode::Fail, limits, true);
    let (entered, release) = gate.unwrap();
    let mut queued = VecDeque::from([Entry::new(1), Entry::new(2), Entry::new(3)]);
    assert_eq!(worker.try_submit_prefix(&mut queued), Ok(1));
    entered.recv_timeout(Duration::from_secs(1)).unwrap();
    release.send(()).unwrap();
    let ApplicationEvent::Failed(failure) = worker.complete().unwrap() else {
        panic!("an application failure cannot acknowledge a prefix");
    };
    assert_eq!(failure.attempted(), &[Entry::new(1)]);
    assert!(failure.unattempted().is_empty());
    assert_eq!(queued, VecDeque::from([Entry::new(2), Entry::new(3)]));
    assert_eq!(
        worker.try_submit_prefix(&mut queued),
        Err(ApplicationSubmitRejection::Stopped)
    );
    assert_eq!(queued, VecDeque::from([Entry::new(2), Entry::new(3)]));
    assert_eq!(worker.durable_through(), LogIndex::ZERO);
    worker.shutdown().unwrap();
}

#[test]
fn an_overflowing_combined_estimate_is_split_without_wrapping() {
    let limits = ApplicationWorkerOptions::new().with_limits(2, usize::MAX, 2, usize::MAX);
    let (mut worker, _, _) = worker(0, Mode::Normal, limits, false);
    let mut queued = VecDeque::from([
        Entry {
            index: LogIndex(1),
            retained_bytes: 1,
            batch_bytes: 1,
        },
        Entry {
            index: LogIndex(2),
            retained_bytes: usize::MAX,
            batch_bytes: usize::MAX,
        },
    ]);
    assert_eq!(worker.try_submit_prefix(&mut queued), Ok(1));
    assert!(matches!(
        worker.complete().unwrap(),
        ApplicationEvent::Applied(_)
    ));
    assert_eq!(worker.try_submit_prefix(&mut queued), Ok(1));
    assert!(matches!(
        worker.complete().unwrap(),
        ApplicationEvent::Applied(_)
    ));
    assert!(queued.is_empty());
    assert_eq!(worker.available(), (2, usize::MAX));
    worker.shutdown().unwrap();
}
