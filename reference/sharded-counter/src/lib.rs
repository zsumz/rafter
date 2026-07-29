//! Deterministic sharded counter service used to discover and verify the
//! contracts a managed many-group Rafter scheduler must meet.
//!
//! The independent model and oracle were written before Rafter had a managed
//! scheduler. The [`adapter`] module now drives real three-node Rafter groups
//! through the promoted managed layer, while the original transition systems
//! remain structurally independent comparison authorities.
//!
//! The implementation and independent oracle share only the public vocabulary
//! in this crate. Their transition code is separate by design, and so is their
//! shape: [`ManagedScheduler`] keeps live books and no history, while
//! [`ReferenceScheduler`] keeps a history and no books.
//!
//! The fairness bound is the point of the whole system. It is stated as a
//! safety property over pass plans rather than as a latency observation, so a
//! test can decide it exactly instead of measuring it approximately.

#![forbid(unsafe_code)]

pub mod adapter;
mod history;
mod model;
mod oracle;
mod types;

pub use history::{HistoryEvent, Operation, OperationId, OperationOutcome};
pub use model::ManagedScheduler;
pub use oracle::{FairnessReport, FaultSite, ReferenceScheduler, Replay, SchedulingViolation};
pub use types::{
    AdmissionOutcome, AdmissionRejection, ClientId, CounterCommand, CounterRejection,
    CounterResult, Delta, FailureRecord, GroupAvailability, GroupId, GroupIncarnation,
    GroupLifecycle, GroupView, LifecycleOutcome, LifecycleRejection, LifecycleRequest,
    LifecycleTransition, Offer, OfferOutcome, PassIndex, PassProgress, PassSuspension,
    ReadinessSignal, RequestFingerprint, RequestIdentity, SchedulerConfig, SchedulerConfigError,
    SchedulerSummary, SchedulerView, Sequence, ServiceCost, ServiceRecord, SessionEpoch,
    SessionOutcome, SkipReason, SystemClass, TickIndex, TickReport, Work, WorkClass, WorkFailure,
    WorkId, WorkQuota, WORK_CLASS_ORDER,
};
