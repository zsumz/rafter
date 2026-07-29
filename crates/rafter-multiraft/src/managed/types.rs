use std::{num::NonZeroU64, sync::Arc};

/// Neutral work classes, in service-priority order within one group turn.
///
/// This enum is exhaustive because the scheduler owns this closed priority
/// vocabulary and every queue has exactly one lane for each variant.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum WorkClass {
    /// Membership, leadership, and other control work.
    Control,
    /// Ordinary application commands.
    Command,
    /// Snapshot construction, transfer, or installation work.
    Snapshot,
    /// Bulk replication or maintenance work.
    Bulk,
}

impl WorkClass {
    pub(super) const COUNT: usize = 4;

    pub(super) const fn index(self) -> usize {
        match self {
            Self::Control => 0,
            Self::Command => 1,
            Self::Snapshot => 2,
            Self::Bulk => 3,
        }
    }

    pub(super) const fn from_index(index: usize) -> Self {
        match index {
            0 => Self::Control,
            1 => Self::Command,
            2 => Self::Snapshot,
            3 => Self::Bulk,
            _ => unreachable!(),
        }
    }
}

/// Stable identity assigned when work is admitted.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct WorkId(NonZeroU64);

impl WorkId {
    pub(super) const fn new(value: NonZeroU64) -> Self {
        Self(value)
    }

    /// Returns the nonzero numeric identity.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Stable identity of one ready-set pass.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PassId(NonZeroU64);

impl PassId {
    pub(super) const fn new(value: NonZeroU64) -> Self {
        Self(value)
    }

    /// Returns the nonzero numeric identity.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Stable identity of one in-flight dispatch.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DispatchId(NonZeroU64);

impl DispatchId {
    pub(super) const fn new(value: NonZeroU64) -> Self {
        Self(value)
    }

    /// Returns the nonzero numeric identity.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Receipt proving one payload took a queue slot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdmissionReceipt {
    /// Stable identity assigned to the accepted payload.
    pub work_id: WorkId,
    /// Queue depth in the selected group after admission.
    pub group_queue_depth: usize,
    /// Total queue depth after admission.
    pub global_queue_depth: usize,
}

/// One immutable ready-set pass plan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PassPlan<G> {
    /// Pass identity.
    pub pass_id: PassId,
    /// Groups ready when the pass was armed, in opportunity order.
    pub groups: Vec<G>,
}

/// Result of asking to arm a ready-set pass.
///
/// This enum is exhaustive because a pass request either creates a pass,
/// observes the existing one, or finds no ready group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ArmPass<G> {
    /// A new pass was armed.
    Armed(PassPlan<G>),
    /// A pass is already open and must be consumed first.
    AlreadyArmed(PassId),
    /// No group is currently ready.
    Idle,
}

/// Work removed from a queue for one dispatch.
#[derive(Debug)]
pub struct DispatchItem<T> {
    /// Admission identity.
    pub work_id: WorkId,
    /// Class used to order this item inside the turn.
    pub class: WorkClass,
    /// Caller-owned work payload.
    pub payload: T,
}

/// One group turn occupying one worker until exact completion.
#[derive(Debug)]
#[must_use = "dropping a dispatch leaves its worker and accepted work in flight"]
pub struct Dispatch<G, T> {
    pub(super) authority: Arc<()>,
    /// Ready-set pass this turn belongs to.
    pub pass_id: PassId,
    /// Stable dispatch identity.
    pub dispatch_id: DispatchId,
    /// Group receiving this opportunity.
    pub group_id: G,
    /// Items selected by class priority, bounded by the group's quota.
    pub items: Vec<DispatchItem<T>>,
}

impl<G, T> Dispatch<G, T>
where
    G: Clone,
{
    /// Creates an opaque permit for this dispatch's exact completion.
    ///
    /// The permit is bound to the scheduler instance, dispatch identity, and
    /// group. Numeric identities from another scheduler cannot release this
    /// dispatch even when both schedulers issued the same numbers.
    #[must_use]
    pub fn completion_permit(&self) -> DispatchCompletionPermit<G> {
        DispatchCompletionPermit {
            authority: Arc::clone(&self.authority),
            dispatch_id: self.dispatch_id,
            group_id: self.group_id.clone(),
        }
    }
}

/// Opaque authority to release one exact dispatch.
#[derive(Debug)]
pub struct DispatchCompletionPermit<G> {
    pub(super) authority: Arc<()>,
    pub(super) dispatch_id: DispatchId,
    pub(super) group_id: G,
}

/// Why a planned group produced no dispatch.
///
/// This enum is exhaustive because availability, occupancy, and queue content
/// are the complete state consulted at a planned opportunity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SkipReason {
    /// The caller marked the group unavailable after the pass was armed.
    Unavailable,
    /// The group already has a dispatch in flight.
    InFlight,
    /// Its queue became empty before its opportunity.
    Empty,
}

/// One planned opportunity that could not service work.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SkippedOpportunity<G> {
    /// Pass containing the opportunity.
    pub pass_id: PassId,
    /// Planned group.
    pub group_id: G,
    /// Observable reason no worker was occupied.
    pub reason: SkipReason,
}

/// Completion of all planned opportunities in a pass.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PassCompletion {
    /// Completed pass.
    pub pass_id: PassId,
    /// Number of planned groups.
    pub planned: usize,
    /// Number of dispatches opened.
    pub dispatched: usize,
    /// Number of planned opportunities skipped.
    pub skipped: usize,
}

/// Result of asking for the next opportunity.
///
/// This enum is exhaustive because it is the scheduler's complete begin-state
/// machine: dispatch, skip, capacity stall, pass completion, or no pass.
#[derive(Debug)]
pub enum BeginDispatch<G, T> {
    /// A worker now owns a nonempty group turn.
    Dispatched(Dispatch<G, T>),
    /// A planned group could not service work.
    Skipped(SkippedOpportunity<G>),
    /// Every worker is occupied; no pass position was consumed.
    WorkersOccupied,
    /// The open pass has no remaining opportunities.
    PassComplete(PassCompletion),
    /// No pass is armed.
    NoPass,
}

/// Terminal disposition of one item in a dispatch.
///
/// This enum is exhaustive because every dispatched item must be accounted for
/// as either serviced or explicitly failed before occupancy can be released.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkDisposition {
    /// The item was serviced.
    Serviced(WorkId),
    /// The item failed explicitly.
    Failed(WorkId),
}

impl WorkDisposition {
    /// Returns the item identity.
    #[must_use]
    pub const fn work_id(self) -> WorkId {
        match self {
            Self::Serviced(id) | Self::Failed(id) => id,
        }
    }
}

/// Exact release of one dispatch occupancy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DispatchCompletion<G> {
    /// Released dispatch.
    pub dispatch_id: DispatchId,
    /// Group whose worker was released.
    pub group_id: G,
    /// Number of serviced items.
    pub serviced: usize,
    /// Number of failed items.
    pub failed: usize,
}

/// Bounded scheduler metrics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedMetrics {
    /// Registered groups.
    pub groups: usize,
    /// Ready groups not already in flight.
    pub ready_groups: usize,
    /// Queued accepted work.
    pub queued: usize,
    /// Accepted work currently in dispatches.
    pub in_flight_work: usize,
    /// Workers currently occupied.
    pub occupied_workers: usize,
    /// Configured workers.
    pub workers: usize,
    /// Passes armed.
    pub passes_armed: u64,
    /// Passes fully offered.
    pub passes_completed: u64,
    /// Total accepted work.
    pub admitted: u64,
    /// Total serviced work.
    pub serviced: u64,
    /// Total explicitly failed work.
    pub failed: u64,
    /// Currently open pass, if any.
    pub open_pass: Option<PassId>,
}
