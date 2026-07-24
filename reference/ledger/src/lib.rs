//! Deterministic ledger used to discover and verify Rafter's public
//! application-facing contracts.
//!
//! The implementation and independent oracle share only the public vocabulary
//! in this crate. Their transition code remains separate by design.

#![forbid(unsafe_code)]

mod history;
mod model;
mod oracle;
mod types;

pub use history::{HistoryEvent, OperationId};
pub use model::{Ledger, LedgerSnapshot, SnapshotError};
pub use oracle::ReferenceLedger;
pub use types::{
    AccountId, Amount, ApplyDisposition, ApplyOutcome, BusinessRejection, ClientId, Command,
    LedgerConfig, LedgerConfigError, LedgerResponse, LedgerSummary, LedgerView, Mutation,
    MutationResult, RequestIdentity, RequestRejection, Sequence, SessionEpoch, SessionView,
};
