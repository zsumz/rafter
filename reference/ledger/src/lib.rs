//! Deterministic ledger used to discover and verify Rafter's public
//! application-facing contracts.
//!
//! The implementation and independent oracle share only the public vocabulary
//! in this crate. Their transition code remains separate by design. The
//! `rafter-app` adapter integrates the implementation only; the oracle stays
//! outside every Rafter-facing path.

#![forbid(unsafe_code)]

mod adapter;
mod history;
mod model;
mod oracle;
mod types;

pub use adapter::{
    LedgerAdapterError, LedgerCodecError, LedgerQuery, LedgerQueryResult, LedgerStateMachine,
    NonZeroField,
};
pub use history::{HistoryEvent, OperationId};
pub use model::{Ledger, LedgerSnapshot, SnapshotError};
pub use oracle::ReferenceLedger;
pub use types::{
    AccountId, Amount, ApplyDisposition, ApplyOutcome, BusinessRejection, ClientId, Command,
    LedgerConfig, LedgerConfigError, LedgerResponse, LedgerSummary, LedgerView, Mutation,
    MutationResult, RequestIdentity, RequestRejection, Sequence, SessionEpoch, SessionView,
};
