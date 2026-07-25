//! Deterministic ledger used to discover and verify Rafter's public
//! application-facing contracts.
//!
//! The implementation and independent oracle share only the public vocabulary
//! in this crate. Their transition code remains separate by design. The
//! `rafter-app` adapter integrates the implementation only; the oracle stays
//! outside every Rafter-facing path.
//!
//! The history checker linearizes recorded client histories against that same
//! oracle. Application invariants and linearizability stay separate checks: a
//! preserved total balance does not show that the observed operations admit a
//! legal real-time ordering.
//!
//! The durable path adds a consumer-written transactional backend in
//! [`store`], and a second `rafter-app` state machine over it. Both machines
//! drive the same pure model and share the same applied-index and snapshot
//! rules; only the location of the state differs.

#![forbid(unsafe_code)]

mod adapter;
mod checker;
mod history;
mod model;
mod oracle;
pub mod store;
mod types;

pub use adapter::{
    DurableLedgerError, DurableLedgerStateMachine, LedgerAdapterError, LedgerCodecError,
    LedgerStateMachine, NonZeroField,
};
pub use checker::{
    check_linearizable, Blocked, BlockedReason, CheckError, CheckReport, HistoryDefect, Violation,
    MAX_HISTORY_OPERATIONS, MAX_SEARCH_CONFIGURATIONS,
};
pub use history::{HistoryEvent, OperationId};
pub use model::{Ledger, LedgerSnapshot, SnapshotError};
pub use oracle::ReferenceLedger;
pub use types::{
    AccountId, Amount, ApplyDisposition, ApplyOutcome, BusinessRejection, ClientId, Command,
    LedgerConfig, LedgerConfigError, LedgerQuery, LedgerQueryResult, LedgerResponse, LedgerSummary,
    LedgerView, Mutation, MutationResult, RequestIdentity, RequestRejection, Sequence,
    SessionEpoch, SessionView,
};
