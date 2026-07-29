//! Deterministic fenced lock service used to discover and verify Rafter's
//! public authority and read contracts.
//!
//! The implementation and independent oracle share only the public vocabulary
//! in this crate. Their transition code remains separate by design, and the
//! guarded resource downstream of both knows nothing but fencing tokens.
//!
//! The adapter carries that model onto Rafter's published crates: a
//! `rafter-app` replicated state machine, versioned command and result frames,
//! and a `rafter-service` client whose queries are always linearizable. It
//! adapts and never re-decides; the oracle stays out of its reach entirely.
//!
//! The `lock-node` binary beside this library runs one replica as an operating
//! system process, over file-backed Raft stores and a TCP link. It is
//! integration composition and nothing more — the link authenticates nothing
//! and the client protocol authenticates nothing — and `CONTRACT.md` says
//! exactly what that establishes.

#![forbid(unsafe_code)]

mod adapter;
mod checker;
mod evidence;
mod guarded;
mod guarded_checker;
mod guarded_history;
mod history;
mod model;
mod oracle;
pub mod production;
pub mod store;
mod types;

pub use adapter::{
    decode_command, decode_result, decode_snapshot, encode_command, encode_result, encode_snapshot,
    unknown_outcome_reason, write_options, DurableLockError, DurableLockStateMachine,
    LockAdapterError, LockClient, LockCodecError, LockHandle, LockQuery, LockQueryResult,
    LockStateMachine, NonZeroField, QueryOutcome, SubmitOutcome,
};
pub use checker::{
    check_linearizable, check_linearizable_with_budget, Blocked, BlockedReason, CheckError,
    CheckReport, HistoryDefect, SearchBudget, Violation, MAX_HISTORY_OPERATIONS,
    MAX_SEARCH_CONFIGURATIONS,
};
pub use evidence::{check_evidence, EvidenceError, EvidenceReport};
pub use guarded::{GuardedRejection, GuardedResource, GuardedWrite};
pub use guarded_checker::{
    check_guarded_history, GuardedCheckError, GuardedCheckReport, GuardedHistoryDefect,
    GuardedViolation,
};
pub use guarded_history::{GuardedHistoryEvent, RecordingGuardedResource};
pub use history::{HistoryEvent, OperationId};
pub use model::{LockService, LockServiceSnapshot, SnapshotError};
pub use oracle::ReferenceLockService;
pub use types::{
    ApplyDisposition, ApplyOutcome, ClientId, Command, FencingToken, LeaseDuration, LockConfig,
    LockConfigError, LockHolderView, LockRejection, LockResponse, LogicalTime, Operation,
    OperationResult, RequestFingerprint, RequestIdentity, RequestRejection, ResourceName,
    ResourceNameError, ResourceStatus, ResourceView, Sequence, ServiceSummary, ServiceView,
    SessionEpoch, SessionView, MAX_RESOURCE_NAME_LEN,
};
