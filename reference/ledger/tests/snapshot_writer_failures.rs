//! Failed creation never returns a publishable descriptor or flushes on drop.

#[path = "support/scratch.rs"]
mod scratch;
#[allow(dead_code)]
#[path = "support/snapshot_writer.rs"]
mod setup;
#[allow(dead_code)]
mod support;

use std::{
    error::Error,
    fmt,
    io::{self, Write},
    panic::{catch_unwind, AssertUnwindSafe},
};

use rafter::{ApplicationSnapshotKind, ApplicationSnapshotVersion, LogIndex};
use rafter_app::state_machine::{ApplicationSnapshotError, ReplicatedStateMachine};
use rafter_reference_ledger::{
    store::{FaultPlan, LedgerStore, LedgerStoreError, WriteFault},
    DurableLedgerError, DurableLedgerStateMachine, LedgerSnapshotWriteError,
};
use rafter_runtime::snapshot_io::{SnapshotCancellation, SnapshotWriteError};

use scratch::ScratchDir;
use setup::{batch, large, metadata, small};

#[derive(Debug)]
struct DiskFailure;
impl fmt::Display for DiskFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("test disk failure")
    }
}
impl Error for DiskFailure {}

#[derive(Default)]
struct Sink {
    calls: usize,
    flushes: usize,
    fail_write: bool,
    fail_flush: bool,
    panic_write: bool,
    cancel: Option<SnapshotCancellation>,
}
impl Write for Sink {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.calls += 1;
        assert!(!self.panic_write, "sink panic");
        if self.fail_write {
            return Err(io::Error::other(DiskFailure));
        }
        if let Some(token) = &self.cancel {
            token.cancel();
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        self.flushes += 1;
        if self.fail_flush {
            return Err(io::Error::other(DiskFailure));
        }
        Ok(())
    }
}

fn typed_disk_error(error: &(dyn Error + 'static)) -> bool {
    if error.is::<DiskFailure>() {
        return true;
    }
    if let Some(io) = error.downcast_ref::<io::Error>() {
        if let Some(inner) = io.get_ref() {
            if typed_disk_error(inner) {
                return true;
            }
        }
    }
    error.source().is_some_and(typed_disk_error)
}

#[test]
fn descriptor_admission_and_prior_cancellation_have_no_sink_effects() {
    for case in 0..4 {
        let mut descriptor = metadata(LogIndex(7));
        let token = SnapshotCancellation::new();
        match case {
            0 => descriptor.last_included_index = LogIndex(8),
            1 => descriptor.application.kind = ApplicationSnapshotKind::new("another-app").unwrap(),
            2 => descriptor.application.version = ApplicationSnapshotVersion::new(2).unwrap(),
            _ => token.cancel(),
        }
        let mut sink = Sink::default();
        let result = small().write_snapshot_to(descriptor, &mut sink, token);
        match case {
            0 => assert!(matches!(
                result,
                Err(LedgerSnapshotWriteError::Payload(
                    SnapshotWriteError::AppliedFloorMismatch { .. }
                ))
            )),
            1 | 2 => assert!(matches!(
                result,
                Err(LedgerSnapshotWriteError::UnsupportedSchema(_))
            )),
            _ => assert!(matches!(
                result,
                Err(LedgerSnapshotWriteError::Payload(
                    SnapshotWriteError::Cancelled
                ))
            )),
        }
        assert_eq!((sink.calls, sink.flushes), (0, 0));
    }
}

#[test]
fn failed_buffer_drain_and_flush_retain_the_original_typed_sink_error() {
    for accounts in [1, 2048] {
        let app = large(accounts);
        for fail_flush in [false, true] {
            let mut sink = Sink {
                fail_write: !fail_flush,
                fail_flush,
                ..Sink::default()
            };
            let error = app
                .write_snapshot_to(
                    metadata(app.applied_index().unwrap()),
                    &mut sink,
                    SnapshotCancellation::new(),
                )
                .err()
                .unwrap();
            assert!(typed_disk_error(&error));
            if fail_flush {
                assert!(matches!(
                    error,
                    LedgerSnapshotWriteError::Payload(SnapshotWriteError::Sink { .. })
                ));
                assert_eq!(sink.flushes, 1);
            } else {
                assert!(matches!(error, LedgerSnapshotWriteError::Write(_)));
                assert_eq!((sink.calls, sink.flushes), (1, 0));
            }
        }
    }
}

#[test]
fn cancellation_after_accepted_bytes_does_not_return_a_descriptor_or_flush() {
    for accounts in [1, 2048] {
        let app = large(accounts);
        let token = SnapshotCancellation::new();
        let mut sink = Sink {
            cancel: Some(token.clone()),
            ..Sink::default()
        };
        assert!(app
            .write_snapshot_to(metadata(app.applied_index().unwrap()), &mut sink, token,)
            .is_err());
        assert_eq!((sink.calls, sink.flushes), (1, 0));
    }
}

#[test]
fn unwinding_a_sink_does_not_retry_buffered_bytes_on_drop() {
    for accounts in [1, 2048] {
        let app = large(accounts);
        let mut sink = Sink {
            panic_write: true,
            ..Sink::default()
        };
        assert!(catch_unwind(AssertUnwindSafe(|| {
            let _ = app.write_snapshot_to(
                metadata(app.applied_index().unwrap()),
                &mut sink,
                SnapshotCancellation::new(),
            );
        }))
        .is_err());
        assert_eq!((sink.calls, sink.flushes), (1, 0));
    }
}

#[test]
fn an_uncertain_journal_write_requires_recovery_before_either_snapshot_builder() {
    let scratch = ScratchDir::new("snapshot-writer-poison");
    let store = LedgerStore::open_with_faults(
        scratch.path(),
        support::config(1, 1),
        FaultPlan::at(1, WriteFault::AtFileSync),
    )
    .unwrap();
    let mut app = DurableLedgerStateMachine::new(store, scratch.path().join("snapshots"));
    assert!(app
        .apply_batch(batch(1, [support::open_session(0, 1)]))
        .is_err());
    assert!(app.store().requires_reopen());
    let mut sink = Sink::default();
    assert!(matches!(
        app.write_snapshot_to(
            metadata(LogIndex(1)),
            &mut sink,
            SnapshotCancellation::new(),
        ),
        Err(LedgerSnapshotWriteError::RequiresReopen)
    ));
    assert!(matches!(
        app.build_snapshot(app.store().applied_index()),
        Err(ApplicationSnapshotError::StateMachine(
            DurableLedgerError::Store(LedgerStoreError::StoreRequiresReopen)
        ))
    ));
    assert_eq!((sink.calls, sink.flushes), (0, 0));
}
