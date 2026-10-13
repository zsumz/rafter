//! The public worker keeps ownership bounded across queue, execution, and completion.

use super::{
    tests::{fixture, proposals, Pipeline, Work},
    PersistenceWorker, PersistenceWorkerOptions, PersistenceWorkerShutdownError, PipelineError,
    PreparedProposals,
};
use rafter::Input;
use std::sync::mpsc::TrySendError;

fn pending() -> (Pipeline, Work) {
    let mut pipeline = Pipeline::new(fixture().0);
    let PreparedProposals::Pending { work, .. } = pipeline.prepare_proposals(proposals()).unwrap()
    else {
        panic!("fixture leader must produce pipelined work")
    };
    (pipeline, work)
}

fn panicking_pending() -> (Pipeline, Work) {
    let mut node = fixture().0;
    node.log_segment.panic_on_append = true;
    let mut pipeline = Pipeline::new(node);
    let PreparedProposals::Pending { work, .. } = pipeline.prepare_proposals(proposals()).unwrap()
    else {
        panic!("fixture leader must produce pipelined work")
    };
    (pipeline, work)
}

#[test]
fn completion_returns_ownership_and_releases_the_single_credit() {
    let (mut pipeline, work) = pending();
    let mut worker = PersistenceWorker::start(PersistenceWorkerOptions::new()).unwrap();
    worker.try_submit(work).unwrap();
    assert!(worker.is_busy());

    let completed = worker.complete().unwrap();
    assert!(!worker.is_busy());
    assert!(completed.storage_telemetry().is_empty());
    pipeline.complete(completed.into_parts().0).unwrap();
    assert!(pipeline.pending_operation().is_none());
    worker.shutdown().unwrap();
}

#[test]
fn second_submission_is_refused_without_losing_its_owned_node() {
    let (mut first, first_work) = pending();
    let (mut second, second_work) = pending();
    let worker = PersistenceWorker::start(PersistenceWorkerOptions::new()).unwrap();
    worker.try_submit(first_work).unwrap();

    let returned = match worker.try_submit(second_work) {
        Err(TrySendError::Full(work)) => work,
        other => panic!("busy worker must return the second work: {other:?}"),
    };
    first
        .complete(worker.complete().unwrap().into_parts().0)
        .unwrap();
    second.complete(returned.persist()).unwrap();
    assert!(second.pending_operation().is_none());
}

#[test]
fn explicit_shutdown_refuses_while_completion_is_unconsumed() {
    let (mut pipeline, work) = pending();
    let mut worker = PersistenceWorker::start(PersistenceWorkerOptions::new()).unwrap();
    worker.try_submit(work).unwrap();
    assert_eq!(worker.shutdown(), Err(PersistenceWorkerShutdownError::Busy));
    pipeline
        .complete(worker.complete().unwrap().into_parts().0)
        .unwrap();
    worker.shutdown().unwrap();
}

#[test]
fn stopped_completion_releases_worker_credit_without_releasing_the_node_fence() {
    let (mut lost, work) = panicking_pending();
    let mut worker = PersistenceWorker::start(PersistenceWorkerOptions::new()).unwrap();
    worker.try_submit(work).unwrap();

    assert!(worker.complete().is_err());
    assert!(!worker.is_busy());
    assert!(lost.pending_operation().is_some());
    assert_eq!(
        lost.step_batch(vec![Input::Tick]),
        Err(PipelineError::PersistencePending)
    );

    let (mut recoverable, work) = pending();
    let returned = match worker.try_submit(work) {
        Err(TrySendError::Disconnected(work)) => work,
        other => panic!("stopped worker must return the submitted work: {other:?}"),
    };
    recoverable.complete(returned.persist()).unwrap();
    assert!(recoverable.pending_operation().is_none());
    assert_eq!(
        worker.shutdown(),
        Err(PersistenceWorkerShutdownError::Panicked)
    );
}

#[test]
fn wake_observes_completion_before_credit_is_released() {
    let (mut pipeline, work) = pending();
    let (notify, wake) = std::sync::mpsc::sync_channel(1);
    let mut worker =
        PersistenceWorker::start_with_wake(PersistenceWorkerOptions::new(), move || {
            let _ = notify.try_send(());
        })
        .unwrap();
    worker.try_submit(work).unwrap();
    wake.recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    assert!(worker.is_busy());
    assert_eq!(worker.shutdown(), Err(PersistenceWorkerShutdownError::Busy));
    let done = worker
        .try_complete()
        .unwrap()
        .expect("publish precedes wake");
    pipeline.complete(done.into_parts().0).unwrap();
    assert!(!worker.is_busy());
    worker.shutdown().unwrap();
    wake.recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    assert!(worker.try_complete().is_err());
}

#[test]
fn storage_panic_disconnects_before_waking_a_sleeping_owner() {
    let (mut pipeline, work) = panicking_pending();
    let (notify, wake) = std::sync::mpsc::sync_channel(1);
    let mut worker =
        PersistenceWorker::start_with_wake(PersistenceWorkerOptions::new(), move || {
            let _ = notify.try_send(());
        })
        .unwrap();
    worker.try_submit(work).unwrap();
    wake.recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    assert!(worker.try_complete().is_err());
    assert_eq!(
        pipeline.step_batch(vec![Input::Tick]),
        Err(PipelineError::PersistencePending)
    );
    let (_, next) = pending();
    assert!(matches!(
        worker.try_submit(next),
        Err(TrySendError::Disconnected(_))
    ));
    assert_eq!(
        worker.shutdown(),
        Err(PersistenceWorkerShutdownError::Panicked)
    );
}
