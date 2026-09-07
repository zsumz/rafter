//! Outcome and refusal types for the managed typed host.
//!
//! Every refusal here hands back the caller's own value — a driver, one group
//! input, or a whole dispatch — so a refused call costs nothing but the
//! attempt. Reports carry each item's full typed result rather than a summary.

use rafter_app::group::{GroupInput, GroupStepReport};

use crate::{error::MultiRaftError, typed::TypedGroupDriver};

use super::{
    AdmissionRejected, CompletionError, Dispatch, DispatchCompletion, DispatchId, RegisterError,
    WorkClass, WorkId,
};

/// Erased typed driver returned when a managed group is removed.
pub type BoxedTypedGroupDriver<G, C, R> =
    Box<dyn TypedGroupDriver<G, Command = C, CommandResult = R>>;

/// Boxed refusal returning one typed group input that took no queue slot.
pub type ManagedAdmissionRejected<G, C> = Box<AdmissionRejected<G, GroupInput<G, C>>>;

/// Why a driver could not be opened in a managed host.
///
/// This enum is exhaustive because opening composes exactly two ownership
/// gates: scheduler registration followed by manual-host adoption.
#[derive(Debug)]
pub enum ManagedOpenError<G> {
    /// The scheduler already owns the group key.
    Scheduler(RegisterError<G>),
    /// The manual typed host refused the driver.
    Host(MultiRaftError<G>),
}

/// A refused managed open that returns the caller's driver.
#[derive(Debug)]
pub struct ManagedOpenRejected<G, D> {
    /// Typed refusal.
    pub error: ManagedOpenError<G>,
    /// Unmodified driver.
    pub driver: D,
}

/// One item outcome from a managed typed-host dispatch.
#[derive(Debug)]
pub struct ManagedItemOutcome<G, R> {
    /// Admission identity.
    pub work_id: WorkId,
    /// Class that selected this item.
    pub class: WorkClass,
    /// Full typed group report or exact host failure.
    pub result: Result<GroupStepReport<G, R>, MultiRaftError<G>>,
}

/// Lossless result of stepping every item in one managed dispatch.
#[derive(Debug)]
#[must_use = "managed reports contain the only copy of each group step report"]
pub struct ManagedDispatchReport<G, R> {
    /// Ready-set pass containing the dispatch.
    pub pass_id: super::PassId,
    /// Dispatch that was stepped.
    pub dispatch_id: DispatchId,
    /// Group that received the turn.
    pub group_id: G,
    /// Per-item typed reports and failures, in dispatch order.
    pub items: Vec<ManagedItemOutcome<G, R>>,
    /// Whether exact scheduler occupancy release succeeded.
    ///
    /// The item reports remain available even if this internal cross-check
    /// fails; no report is summarized away by a scheduling error.
    pub completion: Result<DispatchCompletion<G>, CompletionError>,
}

/// A foreign or stale dispatch refused before any driver was stepped.
#[derive(Debug)]
pub struct ExecuteDispatchRejected<G, C> {
    /// Exact validation failure.
    pub error: CompletionError,
    /// Unmodified dispatch, including every accepted payload.
    pub dispatch: Dispatch<G, GroupInput<G, C>>,
}
