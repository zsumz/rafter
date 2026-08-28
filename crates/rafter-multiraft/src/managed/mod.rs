//! Deterministic bounded scheduling above the manual many-group hosts.
//!
//! The scheduler owns admission, ready-set passes, quotas, work-class order,
//! and explicit dispatch occupancy. It contains no threads, clocks, sockets,
//! storage, application lifecycle, or retry policy.

mod config;
mod error;
mod identity;
mod report;
mod scheduler;
mod typed;
mod types;

pub use config::{ManagedConfig, ManagedConfigError};
pub use error::{
    AdmissionRejected, AdmissionRejection, CompletionError, GroupStateError, IdentityError,
    RegisterError, RemoveError,
};
pub use identity::{DispatchId, PassId, WorkClass, WorkId};
pub use report::{
    BoxedTypedGroupDriver, ExecuteDispatchRejected, ManagedAdmissionRejected,
    ManagedDispatchReport, ManagedItemOutcome, ManagedOpenError, ManagedOpenRejected,
};
pub use scheduler::ManagedScheduler;
pub use typed::ManagedTypedMultiRaftHost;
pub use types::{
    AdmissionReceipt, ArmPass, BeginDispatch, Dispatch, DispatchCompletion,
    DispatchCompletionPermit, DispatchItem, FailedQueuedItem, ManagedMetrics, PassCompletion,
    PassPlan, SkipReason, SkippedOpportunity, WorkDisposition,
};
