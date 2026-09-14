//! Credit accounting and worker lifecycle proofs.

use super::*;

#[test]
fn credits_refuse_overflow_and_limit_excess() {
    let credits = AtomicUsize::new(3);
    assert!(!reserve(&credits, 2, 4));
    assert_eq!(credits.load(Ordering::Relaxed), 3);
    assert!(!reserve(&credits, usize::MAX, usize::MAX));
    assert_eq!(credits.load(Ordering::Relaxed), 3);
    assert!(reserve(&credits, 1, 4));
    assert_eq!(credits.load(Ordering::Relaxed), 4);
}

#[test]
fn zero_limits_are_refused_and_empty_work_is_accepted() {
    assert!(LogRetirementWorker::start(LogRetirementWorkerOptions::new(0, 1)).is_err());
    let mut worker = LogRetirementWorker::start(LogRetirementWorkerOptions::new(1, 1))
        .expect("nonzero credits start the worker");
    worker
        .try_submit(RetiredLogEntries::default())
        .expect("empty work is a no-op");
    assert_eq!(worker.inflight_entries(), 0);
    worker.shutdown().expect("the idle worker joins");
    assert!(matches!(
        worker.try_submit(RetiredLogEntries::default()),
        Ok(())
    ));
}
