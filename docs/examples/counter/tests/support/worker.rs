//! Application-worker integration retains ranges and releases durable results.
use super::{application::Entry, counter::Counter, Scratch};
use rafter::{LocalProposalId, LogIndex, Term};
use rafter_runtime::{
    application::{
        ApplicationEntry, ApplicationEvent, ApplicationFailureKind, ApplicationWorker,
        ApplicationWorkerOptions,
    },
    service::CommittedCommand,
};
use std::{
    fs, thread,
    time::{Duration, Instant},
};

fn command(first: u64, index: u64, bytes: Vec<u8>) -> CommittedCommand {
    CommittedCommand {
        first_index: LogIndex(first),
        index: LogIndex(index),
        term: Term(7),
        payload: bytes.into(),
        local_proposal_id: Some(LocalProposalId(index + 100)),
    }
}
fn entry(first: u64, index: u64, amount: u64) -> Entry {
    Entry::try_from(command(first, index, amount.to_be_bytes().to_vec())).unwrap()
}
fn complete(
    worker: &ApplicationWorker<Entry, Counter>,
) -> ApplicationEvent<Entry, rafter_app::state_machine::ApplyResult<u64>, std::io::Error> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(event) = worker.try_complete().unwrap() {
            return event;
        }
        assert!(Instant::now() < deadline, "application worker stalled");
        thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn adapter_refuses_malformed_commands() {
    for len in [0, 7, 9, 65_536] {
        assert!(Entry::try_from(command(1, 2, vec![0; len])).is_err());
    }
}

#[test]
fn worker_preserves_protocol_prefixes_results_and_durable_progress() {
    let scratch = Scratch::new("worker");
    let first = entry(1, 2, 5);
    let bytes = first.retained_bytes() * 2;
    let mut worker = ApplicationWorker::start(
        scratch.open(),
        ApplicationWorkerOptions::new().with_limits(2, bytes, 2, bytes),
        || {},
    )
    .unwrap();
    worker.try_submit(vec![first, entry(3, 5, 3)]).unwrap();
    assert_eq!(worker.available(), (0, 0));
    let ApplicationEvent::Applied(done) = complete(&worker) else {
        panic!("expected durable completion");
    };
    assert_eq!(done.entries()[0].first_log_index(), LogIndex(1));
    assert_eq!(done.entries()[1].first_log_index(), LogIndex(3));
    assert_eq!(
        done.outcomes().iter().map(|r| r.result).collect::<Vec<_>>(),
        [5, 8]
    );
    let result = &done.outcomes()[1];
    assert_eq!(
        (result.index, result.term, result.local_proposal_id),
        (LogIndex(5), Term(7), Some(LocalProposalId(105)))
    );
    assert_eq!(worker.durable_through(), LogIndex(5));
    assert_eq!(worker.available(), (2, bytes));
    drop(worker.shutdown_into_store().unwrap());
    let saved: super::State = super::disk::load(&scratch.0.join("counter")).unwrap();
    assert_eq!((saved.applied, saved.value), (5, 8));
}

#[test]
fn failed_worker_publication_never_reports_success_or_advances_saved_floor() {
    let scratch = Scratch::new("worker-failure");
    let mut app = scratch.open();
    use rafter_app::state_machine::ReplicatedStateMachine;
    app.apply_batch(super::increment(2, 5)).unwrap();
    fs::create_dir(scratch.0.join("counter.tmp")).unwrap();
    let mut worker = ApplicationWorker::start(app, ApplicationWorkerOptions::new(), || {}).unwrap();
    worker.try_submit(vec![entry(3, 4, 3)]).unwrap();
    let ApplicationEvent::Failed(failed) = complete(&worker) else {
        panic!("failed atomic publication must not acknowledge the command");
    };
    assert!(matches!(failed.kind(), ApplicationFailureKind::Store(_)));
    assert_eq!(failed.attempted()[0].log_index(), LogIndex(4));
    assert_eq!(worker.durable_through(), LogIndex(2));
    assert!(!worker.is_accepting());
    let returned = worker.shutdown_into_store().unwrap();
    assert_eq!(returned.value(), 5);
    assert_eq!(returned.applied_index().unwrap(), LogIndex(2));
    let saved: super::State = super::disk::load(&scratch.0.join("counter")).unwrap();
    assert_eq!((saved.applied, saved.value), (2, 5));
}
