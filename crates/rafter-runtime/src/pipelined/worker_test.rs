//! The public worker keeps ownership bounded across queue, execution, and completion.

use super::{
    tests::{fixture, proposals, Pipeline, Work},
    PersistenceWorker, PersistenceWorkerOptions, PersistenceWorkerShutdownError, PreparedProposals,
};
use std::sync::mpsc::TrySendError;

fn pending() -> (Pipeline, Work) {
    let mut pipeline = Pipeline::new(fixture().0);
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
