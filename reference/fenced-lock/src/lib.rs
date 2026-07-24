//! Deterministic fenced lock service used to discover and verify Rafter's
//! public authority and read contracts.
//!
//! The implementation and independent oracle share only the public vocabulary
//! in this crate. Their transition code remains separate by design, and the
//! guarded resource downstream of both knows nothing but fencing tokens.

#![forbid(unsafe_code)]

mod guarded;
mod history;
mod model;
mod oracle;
mod types;

pub use guarded::{GuardedRejection, GuardedResource, GuardedWrite};
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
