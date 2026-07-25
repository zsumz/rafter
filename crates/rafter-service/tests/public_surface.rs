//! The crate root is the whole public surface.
//!
//! `rafter-service` re-exports its public surface from the crate root, and the
//! rule this file enforces is that **every type or trait named in the signature
//! of a public item is reachable from the crate root of the crate that declares
//! that item**. The consequence for a consumer is concrete: implementing
//! `DriverCommandSender` requires naming `DriverFuture`, and before this rule
//! was adopted that name existed only behind a module path the crate's own
//! convention hid.
//!
//! Compilation is the assertion. This file uses `rafter_service::` root paths
//! and nothing else — no `rafter_service::driver::` and no `rafter_service::
//! error::` anywhere below — so a type that falls off the re-export list breaks
//! this test rather than the next consumer's import block.

use std::future::ready;

use rafter::{LogIndex, NodeId, Term};
use rafter_service::{
    DriverCommandSender, DriverFuture, ErrorCause, MetricsError, MetricsWatch, QueryReceipt,
    ReadConsistency, ReadError, ShutdownError, StateMachineOperation, TransferLeadershipError,
    WriteError, WriteOptions, WriteReceipt,
};

/// A driver that refuses everything, written the way an external embedder would
/// have to write one: entirely out of names the crate root exports.
#[derive(Clone, Debug)]
struct RefusingDriver;

impl DriverCommandSender<u64, Vec<u8>, Vec<u8>, (), ()> for RefusingDriver {
    fn write(
        &self,
        _group_id: u64,
        _command: Vec<u8>,
        _options: WriteOptions,
    ) -> DriverFuture<Result<WriteReceipt<()>, WriteError>> {
        Box::pin(ready(Err(WriteError::ShuttingDown)))
    }

    fn read(
        &self,
        _group_id: u64,
        _query: Vec<u8>,
        _consistency: ReadConsistency,
    ) -> DriverFuture<Result<QueryReceipt<u64, ()>, ReadError>> {
        Box::pin(ready(Err(ReadError::ShuttingDown)))
    }

    fn transfer_leadership(
        &self,
        _group_id: u64,
        _target: NodeId,
    ) -> DriverFuture<Result<(), TransferLeadershipError>> {
        Box::pin(ready(Err(TransferLeadershipError::ShuttingDown)))
    }

    fn metrics(&self, _group_id: u64) -> Result<MetricsWatch<u64>, MetricsError> {
        Err(MetricsError::WrongGroup)
    }

    fn shutdown(&self, _group_id: u64) -> DriverFuture<Result<(), ShutdownError>> {
        Box::pin(ready(Err(ShutdownError::AlreadyShutDown)))
    }
}

/// The entry, made executable: this impl block does not compile without
/// `DriverFuture` on the root's re-export list.
#[test]
fn the_driver_boundary_is_nameable_from_the_crate_root() {
    let driver = RefusingDriver;

    assert!(matches!(driver.metrics(7), Err(MetricsError::WrongGroup)));

    let receipt: WriteReceipt<()> = WriteReceipt {
        index: LogIndex(1),
        term: Term(1),
        result: (),
    };
    assert_eq!(receipt.index, LogIndex(1));
}

/// `StateMachineOperation` and `ErrorCause` are declared in `rafter-app` and
/// carried by this crate's public error variants, so the root must export the
/// same types rather than redeclare them. Two types with one name and no
/// conversion between them would break a caller that compares the value it
/// received with the one the app layer produced.
#[test]
fn a_service_error_is_matchable_from_the_crate_root() {
    // Declared with the app layer's paths, consumed with the root's: the two
    // annotations only agree because there is one type.
    fn app_operation() -> rafter_app::error::StateMachineOperation {
        rafter_app::error::StateMachineOperation::EncodeCommand
    }

    fn app_cause() -> rafter_app::error::ErrorCause {
        rafter_app::error::ErrorCause::new(MetricsError::WrongGroup)
    }

    let operation: StateMachineOperation = StateMachineOperation::EncodeCommand;
    assert_eq!(operation, app_operation());

    let cause: ErrorCause = app_cause();
    assert!(cause.downcast_ref::<MetricsError>().is_some());
}
