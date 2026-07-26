use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, VecDeque},
};

use crate::{
    AdmissionOutcome, AdmissionRejection, ClientId, CounterCommand, CounterRejection,
    CounterResult, FailureRecord, GroupAvailability, GroupId, GroupIncarnation, GroupLifecycle,
    GroupView, HistoryEvent, LifecycleOutcome, LifecycleRejection, LifecycleRequest, OfferOutcome,
    Operation, OperationOutcome, PassIndex, RequestFingerprint, RequestIdentity, SchedulerConfig,
    SchedulerSummary, SchedulerView, Sequence, ServiceRecord, SessionEpoch, SessionOutcome,
    SkipReason, TickIndex, Work, WorkClass, WorkFailure, WorkId, WorkQuota,
};

/// A scheduling rule the recorded decisions did not keep.
///
/// Every variant names the group and the pass, because a fairness failure a
/// report could not point at is a benchmark impression rather than a proof.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchedulingViolation {
    /// A ready group was left out of a pass plan.
    ///
    /// This is the fairness bound broken. `denied_passes` is the longest run of
    /// consecutive plans that omitted the group while it was ready, and
    /// `from_pass` is where that run began. The bound is `denied_passes == 0`,
    /// so any report at all is a failure; the size says how badly.
    OpportunityGap {
        /// Group that was denied its turn.
        group: GroupId,
        /// First pass of the longest denial run.
        from_pass: PassIndex,
        /// Length of that run, counted in armed plans.
        denied_passes: u32,
    },
    /// A plan was armed while an earlier plan still owed a group its turn.
    ///
    /// This is the other half of the bound: a pass that may be abandoned proves
    /// nothing about the groups it had not reached.
    PassArmedWhileOpen {
        /// Pass still owed turns.
        open: PassIndex,
        /// Pass that was armed anyway.
        armed: PassIndex,
    },
    /// A pass index did not follow its predecessor.
    PassOutOfOrder {
        /// Pass that was expected next.
        expected: PassIndex,
        /// Pass that arrived.
        observed: PassIndex,
    },
    /// A plan named a group that was not ready when it was armed.
    PlanIncludedUnreadyGroup {
        /// Pass whose plan named it.
        pass: PassIndex,
        /// Group that was not ready.
        group: GroupId,
    },
    /// A plan named one group more than once.
    PlanRepeatedGroup {
        /// Pass whose plan repeated it.
        pass: PassIndex,
        /// Group that appeared twice.
        group: GroupId,
    },
    /// A turn was taken outside the open plan.
    OfferOutsidePlan {
        /// Pass the turn claimed.
        pass: PassIndex,
        /// Group that took it.
        group: GroupId,
    },
    /// A group took a second turn within one pass.
    GroupOfferedTwice {
        /// Pass in which it happened.
        pass: PassIndex,
        /// Group that took two turns.
        group: GroupId,
    },
    /// A pass retired while a group in its plan still had no turn.
    PassCompletedWithUnofferedGroup {
        /// Pass that retired early.
        pass: PassIndex,
        /// Group that never got its turn.
        group: GroupId,
    },
    /// A group was dispatched although it was not ready.
    DispatchedUnreadyGroup {
        /// Pass in which it happened.
        pass: PassIndex,
        /// Group that should not have been dispatched.
        group: GroupId,
    },
    /// A group was skipped as stalled although it was available.
    SkippedAvailableGroup {
        /// Pass in which it happened.
        pass: PassIndex,
        /// Group that was skipped.
        group: GroupId,
    },
    /// An opportunity serviced more items than the group's quota allows.
    QuotaExceeded {
        /// Pass in which it happened.
        pass: PassIndex,
        /// Group that overran.
        group: GroupId,
        /// Items the opportunity serviced.
        serviced: u32,
        /// Items its quota allowed.
        quota: u32,
    },
    /// An opportunity serviced work out of its priority or arrival order.
    ///
    /// Servicing a lower-priority item while a higher-priority one waits is the
    /// class rule broken; servicing a later item of the same class first is the
    /// arrival rule broken. Both look the same from here: the wrong item moved.
    ServiceOrderViolation {
        /// Pass in which it happened.
        pass: PassIndex,
        /// Group that serviced the wrong item.
        group: GroupId,
        /// Item that was owed the slot.
        expected: WorkId,
        /// Item that took it.
        observed: WorkId,
    },
    /// An opportunity serviced fewer or more items than it claimed.
    ///
    /// `expected` is the number of [`crate::HistoryEvent::WorkServiced`]
    /// events the turn actually recorded, and `observed` is the number its
    /// dispatch claimed. The two are compared rather than either being
    /// believed, because a turn that reports work it did not do is exactly how
    /// an occupancy gets bought with nothing.
    ServiceCountMismatch {
        /// Pass in which it happened.
        pass: PassIndex,
        /// Group that miscounted.
        group: GroupId,
        /// Items the turn recorded as serviced.
        expected: u32,
        /// Items its dispatch claimed.
        observed: u32,
    },
    /// A turn ended owing work it never serviced.
    ///
    /// A dispatch is offered against a queue, and the items at the head of that
    /// queue are what the turn is for. Ending the turn with any of them still
    /// queued means the worker was held for work that never moved — which,
    /// before this was checked, bought a full occupancy with nothing and kept
    /// the group out of the ready set for the whole of it.
    ///
    /// A turn ends at the first recorded event that is not one of its own
    /// services: another dispatch, a release, a plan retiring, or the end of
    /// the history. Work is applied at dispatch, so a turn's services are the
    /// dispatch itself and nothing may fall between them.
    DispatchLeftWorkUnserviced {
        /// Pass in which it happened.
        pass: PassIndex,
        /// Group that kept its queue.
        group: GroupId,
        /// Items the turn was offered against.
        owed: u32,
        /// Items it serviced.
        serviced: u32,
    },
    /// A turn serviced more items than the queue it was offered against held.
    ///
    /// The mirror of [`Self::DispatchLeftWorkUnserviced`], and not a harmless
    /// generosity either: a turn that runs past its own quota is a group taking
    /// throughput share the pass never granted it.
    DispatchServicedBeyondItsWork {
        /// Pass in which it happened.
        pass: PassIndex,
        /// Group that overran its turn.
        group: GroupId,
        /// Items the turn was offered against.
        owed: u32,
        /// Item it serviced beyond them.
        work: WorkId,
    },
    /// An opportunity charged its worker something other than its work cost.
    ///
    /// The cost of a turn is the sum of the [`crate::ServiceCost`]s of the
    /// items it serviced, and the fold knows those items because it read every
    /// [`crate::HistoryEvent::WorkServiced`] the turn recorded. A dispatch that
    /// reports any other number is either under-charging a worker it is
    /// holding or over-charging one it is not.
    DispatchCostMismatch {
        /// Pass in which it happened.
        pass: PassIndex,
        /// Group whose turn it was.
        group: GroupId,
        /// Occupancy the serviced items add up to.
        expected: u64,
        /// Occupancy the dispatch claimed.
        observed: u64,
    },
    /// A worker occupancy outlived the cost that opened it.
    ///
    /// This is the starvation the fairness bound could not otherwise see. A
    /// group held in an occupancy that never ends is never ready, is therefore
    /// owed no turn, and accrues no gap — so the audit stays green while the
    /// group receives nothing. The occupancy's end is derived instead: it is
    /// due at the dispatch tick plus the dispatch's derived cost, and a
    /// history that runs past that instant with the occupancy still open has
    /// broken the contract, whether or not a release ever arrives.
    WorkerHeldPastCost {
        /// Pass whose dispatch opened the occupancy.
        pass: PassIndex,
        /// Group still holding the worker.
        group: GroupId,
        /// Tick the release was due at.
        due: TickIndex,
        /// Tick the history had reached.
        observed: TickIndex,
    },
    /// A worker occupancy ended before the cost that opened it was paid.
    ///
    /// The mirror of [`Self::WorkerHeldPastCost`], and not a harmless
    /// generosity: an early release returns a worker that is still busy to the
    /// pool, so the host can hold more dispatches at once than it has workers,
    /// and returns its group to the ready set having paid less than its work
    /// cost.
    WorkerReleasedEarly {
        /// Pass whose dispatch opened the occupancy.
        pass: PassIndex,
        /// Group released early.
        group: GroupId,
        /// Tick the release was due at.
        due: TickIndex,
        /// Tick the release arrived at.
        observed: TickIndex,
    },
    /// A worker was released for a group that was holding none.
    ///
    /// There is no pass to name, which is the whole complaint: the release
    /// belongs to no dispatch. Absorbing it silently would let a scheduler
    /// clear an occupancy it never opened, and the readiness that occupancy
    /// governs with it.
    SpuriousWorkerRelease {
        /// Tick the release claimed.
        tick: TickIndex,
        /// Group it named.
        group: GroupId,
    },
    /// More groups held workers at once than the configuration has workers.
    WorkerCountExceeded {
        /// Pass whose dispatch overran the pool.
        pass: PassIndex,
        /// Group that took the worker there was not.
        group: GroupId,
        /// Workers the configuration provides.
        workers: u32,
    },
    /// A recorded decision claimed a tick earlier than one already recorded.
    ///
    /// Ticks are the fold's only clock. A history that walks one backwards
    /// could park an occupancy's deadline permanently in the future, so the
    /// clock is checked rather than trusted. This names no group because the
    /// fault is the history's shape rather than any one group's treatment.
    TickWentBackwards {
        /// Tick the fold had already reached.
        current: TickIndex,
        /// Tick the event claimed.
        observed: TickIndex,
    },
    /// Two plans were armed, or two retired, within one tick.
    ///
    /// A tick arms at most one plan and retires at most one. The rule keeps
    /// the pass-to-tick relationship crisp, and it is what stops a scheduler
    /// from arming an unbounded number of plans — starving a group across
    /// every one of them — while its clock, and therefore every occupancy
    /// deadline it owes, stands still.
    PassBoundaryReused {
        /// Pass that reused the boundary.
        pass: PassIndex,
        /// Tick it reused.
        tick: TickIndex,
    },
    /// Work was serviced outside any dispatch.
    ServiceOutsideDispatch {
        /// Pass the service claimed.
        pass: PassIndex,
        /// Group that claimed it.
        group: GroupId,
    },
    /// Accepted work neither serviced, retired, nor still queued.
    ///
    /// The conservation law: every admitted item reaches exactly one terminal
    /// disposition, and this is what it looks like when one does not.
    WorkNotConserved {
        /// Items admitted over the history.
        admitted: u64,
        /// Items serviced.
        serviced: u64,
        /// Items retired without service.
        failed: u64,
        /// Items still queued.
        queued: u32,
    },
}

/// One place in the fold where a rule is decided.
///
/// [`SchedulingViolation`] names the rule that broke. This names the *check
/// that caught it*, and the two are deliberately not in bijection: there are
/// more checks than variants, because five checks reuse a variant another check
/// already reports. A red-team suite that gave every variant a deliberate
/// violator therefore left three checks with none — the two retire-side
/// pass-ordering checks, the retire-side tick-reuse check, and the release-side
/// held-past-cost check — while claiming, in an exhaustive match over
/// `SchedulingViolation`, that the compiler had ruled that out. The claim was
/// one scope wider than the mechanism: exhaustive matching closes *variants*,
/// and a rule is a site.
///
/// So the site is what the audit reports alongside the violation, and what
/// `tests/redteam_controls.rs` matches exhaustively. Adding a rule now means
/// adding a site, and adding a site without a scheduler that provokes it stops
/// that suite compiling — including when the rule reuses a variant that already
/// had a control.
///
/// # What this does not close
///
/// It closes *omission*: a check with no site, or a site with no control, does
/// not compile. It does not close a false statement — an author who adds a
/// second `fault` call passing a site that already belongs to another check has
/// said something untrue about their own code, and no arrangement of Rust
/// enums detects that without variant reflection this crate has no dependency
/// to obtain. `every_fault_site_is_raised_by_exactly_one_check` scans this
/// file's own source for the remaining case.
#[allow(
    clippy::manual_non_exhaustive,
    reason = "the marker's discriminant is the site count, which `#[non_exhaustive]` \
              does not provide — and `#[non_exhaustive]` would force a wildcard arm on \
              every match over this type, which is the exact guarantee it exists to give"
)]
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum FaultSite {
    /// The clock check, in `Fold::monotone`.
    ClockWalkedBackwards,
    /// Calling in an overdue occupancy as the clock advances, in `Fold::reach`.
    OccupancyOutlivedItsCost,
    /// A release naming a group holding no worker, in `Fold::release`.
    ReleaseWithoutOccupancy,
    /// A release before its occupancy's deadline, in `Fold::release`.
    ReleaseBeforeDue,
    /// A release after its occupancy's deadline, in `Fold::release`.
    ///
    /// Distinct from [`Self::OccupancyOutlivedItsCost`], which reports the same
    /// variant for an occupancy no release ever came for.
    ReleaseAfterDue,
    /// Arming while a plan is still open, in `Fold::arm`.
    ArmedOverAnOpenPass,
    /// Arming a pass index that does not follow its predecessor, in `Fold::arm`.
    ArmedOutOfOrder,
    /// Arming twice within one tick, in `Fold::arm`.
    ArmedTickReused,
    /// A plan naming one group twice, in `Fold::arm`.
    PlanRepeatedAGroup,
    /// A plan naming a group that was not ready, in `Fold::arm`.
    PlanNamedAnUnreadyGroup,
    /// A turn taken outside the open plan, in `Fold::offer`.
    OfferOutsideThePlan,
    /// A second turn for a group within one pass, in `Fold::offer`.
    OfferedTwiceInOnePass,
    /// A turn offered to a group that was never created, in `Fold::offer`.
    OfferedAnUnknownGroup,
    /// A skip claimed for a group nothing reported stalled, in `Fold::offer`.
    SkippedWhileAvailable,
    /// A dispatch of a group that was not ready, in `Fold::offer`.
    DispatchedWhileUnready,
    /// A turn servicing more than the group's quota, in `Fold::offer`.
    DispatchOverQuota,
    /// More concurrent turns than the pool has workers, in `Fold::offer`.
    DispatchOverWorkerPool,
    /// A turn ending with the work it was offered against still queued, in
    /// `Fold::settle`.
    TurnLeftWorkQueued,
    /// A turn claiming a different count than it recorded, in `Fold::settle`.
    TurnMiscountedItsWork,
    /// A turn claiming a different cost than its work was worth, in
    /// `Fold::settle`.
    TurnMispricedItsWork,
    /// Work serviced with no turn open to service it, in `Fold::service`.
    ServiceOutsideATurn,
    /// Work serviced past the end of a turn's offered items, in
    /// `Fold::service`.
    ServicePastTheTurnsWork,
    /// Work serviced out of priority or arrival order, in `Fold::service`.
    ServiceOutOfOrder,
    /// Retiring twice within one tick, in `Fold::complete`.
    ///
    /// Distinct from [`Self::ArmedTickReused`], which reports the same variant
    /// for the arming half of the same rule.
    RetiredTickReused,
    /// Retiring when no plan is open, in `Fold::complete`.
    RetiredWithNoPassOpen,
    /// Retiring a pass other than the open one, in `Fold::complete`.
    RetiredADifferentPass,
    /// Retiring a plan that still owes a group its turn, in `Fold::complete`.
    RetiredWithATurnOwing,
    /// The conservation self-check, in `Fold::finish`.
    WorkUnaccountedFor,
    /// The fairness bound itself, in `Fold::widest_gap`.
    ///
    /// The only site that is not a `Fold::fault` call, and it is here because
    /// the registry covers every place the audit produces a violation rather
    /// than every place it calls one particular method.
    WidestOpportunityGap,
    /// **Not a site.** The end marker, and the only thing that makes
    /// [`Self::ALL`] closed under adding one.
    ///
    /// Its discriminant *is* the number of sites, so a variant added above it
    /// changes `ALL`'s length and fails the const check below unless it is also
    /// threaded onto [`Self::next`]. It must stay last, and `ALL` never
    /// contains it. A variant declared *after* it escapes — which is a
    /// statement that this marker is not the end, not an omission.
    #[doc(hidden)]
    EndOfSites,
}

impl FaultSite {
    /// The number of sites, taken from the end marker's discriminant.
    pub const COUNT: usize = Self::EndOfSites as usize;

    /// Every site, in declaration order.
    pub const ALL: [Self; Self::COUNT] = Self::all();

    /// The next site in declaration order, or `None` past the last.
    ///
    /// Exhaustive, with no catch-all: a site added without an arm here does not
    /// compile.
    const fn next(self) -> Option<Self> {
        Some(match self {
            Self::ClockWalkedBackwards => Self::OccupancyOutlivedItsCost,
            Self::OccupancyOutlivedItsCost => Self::ReleaseWithoutOccupancy,
            Self::ReleaseWithoutOccupancy => Self::ReleaseBeforeDue,
            Self::ReleaseBeforeDue => Self::ReleaseAfterDue,
            Self::ReleaseAfterDue => Self::ArmedOverAnOpenPass,
            Self::ArmedOverAnOpenPass => Self::ArmedOutOfOrder,
            Self::ArmedOutOfOrder => Self::ArmedTickReused,
            Self::ArmedTickReused => Self::PlanRepeatedAGroup,
            Self::PlanRepeatedAGroup => Self::PlanNamedAnUnreadyGroup,
            Self::PlanNamedAnUnreadyGroup => Self::OfferOutsideThePlan,
            Self::OfferOutsideThePlan => Self::OfferedTwiceInOnePass,
            Self::OfferedTwiceInOnePass => Self::OfferedAnUnknownGroup,
            Self::OfferedAnUnknownGroup => Self::SkippedWhileAvailable,
            Self::SkippedWhileAvailable => Self::DispatchedWhileUnready,
            Self::DispatchedWhileUnready => Self::DispatchOverQuota,
            Self::DispatchOverQuota => Self::DispatchOverWorkerPool,
            Self::DispatchOverWorkerPool => Self::TurnLeftWorkQueued,
            Self::TurnLeftWorkQueued => Self::TurnMiscountedItsWork,
            Self::TurnMiscountedItsWork => Self::TurnMispricedItsWork,
            Self::TurnMispricedItsWork => Self::ServiceOutsideATurn,
            Self::ServiceOutsideATurn => Self::ServicePastTheTurnsWork,
            Self::ServicePastTheTurnsWork => Self::ServiceOutOfOrder,
            Self::ServiceOutOfOrder => Self::RetiredTickReused,
            Self::RetiredTickReused => Self::RetiredWithNoPassOpen,
            Self::RetiredWithNoPassOpen => Self::RetiredADifferentPass,
            Self::RetiredADifferentPass => Self::RetiredWithATurnOwing,
            Self::RetiredWithATurnOwing => Self::WorkUnaccountedFor,
            Self::WorkUnaccountedFor => Self::WidestOpportunityGap,
            Self::WidestOpportunityGap | Self::EndOfSites => return None,
        })
    }

    const fn all() -> [Self; Self::COUNT] {
        let mut sites = [Self::ClockWalkedBackwards; Self::COUNT];
        let mut index = 0;
        let mut current = Some(Self::ClockWalkedBackwards);
        while index < Self::COUNT {
            let Some(site) = current else { break };
            sites[index] = site;
            current = site.next();
            index += 1;
        }
        sites
    }
}

// `ALL` walks `next` from the first variant, and this is what makes that walk a
// closure claim rather than a hope: every entry must sit at its own declaration
// index, so a chain that stops early, repeats itself, or skips a variant leaves
// a slot holding the wrong site and fails to compile.
const _: () = {
    let mut index = 0;
    while index < FaultSite::COUNT {
        assert!(
            FaultSite::ALL[index] as usize == index,
            "FaultSite::next must visit every site once, in declaration order"
        );
        index += 1;
    }
};

/// Evidence that the recorded decisions kept the scheduling contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FairnessReport {
    /// Plans armed over the history.
    pub passes_armed: u64,
    /// Plans that retired with every entry offered.
    pub passes_completed: u64,
    /// Turns handed out.
    pub opportunities: u64,
    /// Items the recorded turns serviced.
    ///
    /// This is the floor a fairness assertion needs, and it is deliberately
    /// not `passes_completed`. Arming a plan is free — an empty plan names
    /// exactly the ready set when nothing is ready — so a host wedged behind
    /// one expensive item can retire plans forever while doing nothing, and a
    /// floor on plans certifies it. A floor on the work that moved cannot be
    /// met by a host that moved none. See CONTRACT.md, "What the audit does
    /// not claim".
    pub serviced: u64,
    /// Largest plan armed.
    pub widest_plan: u32,
    /// Largest run of armed plans any ready group went without a turn.
    ///
    /// Readiness is sampled at each arming, so the run is counted in plans
    /// armed rather than in passes retired. That is the stricter of the two: a
    /// plan that omitted a ready group and then never completed is still
    /// counted here, where a pass-based count would have lost it.
    ///
    /// The bound is that this is zero. It is reported rather than merely
    /// asserted so that a green run says which number it proved.
    pub widest_gap: u32,
}

/// Everything one replay of a recorded history derives.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Replay {
    /// State the history implies.
    pub view: SchedulerView,
    /// Aggregate counts the history implies.
    pub summary: SchedulerSummary,
    /// Outcome each invoked operation should have produced, in invocation
    /// order.
    pub outcomes: Vec<OperationOutcome>,
    /// Service records the recorded dispatches should have produced, in order.
    pub services: Vec<ServiceRecord>,
    /// Failure records the recorded drains should have produced, in order.
    pub failures: Vec<FailureRecord>,
    /// Whether the recorded decisions kept the scheduling contract.
    pub fairness: Result<FairnessReport, SchedulingViolation>,
    /// The check that reported `fairness`'s violation, when there is one.
    ///
    /// A violation names the rule that broke; this names the rule that *caught*
    /// it, which is not the same question wherever two checks report one
    /// variant. `tests/redteam_controls.rs` is the reason it is carried:
    /// without it, a suite can only claim a control for every variant, and
    /// three of this fold's checks had none while that claim was being made by
    /// an exhaustive match.
    pub fault: Option<FaultSite>,
}

/// Structurally independent executable specification for the managed
/// scheduler.
///
/// This oracle schedules nothing. It keeps an append-only history and answers
/// every question by folding it: the counters, the lifecycles, the queues, the
/// ready set at each arming, and the per-group opportunity gaps are all
/// consequences of the recorded events rather than books kept alongside them.
/// [`crate::ManagedScheduler`] is the opposite in every one of those places — a
/// dense slot table, four class queues per group, an incrementally maintained
/// ready set, and no history at all — so a bookkeeping mistake in either has
/// nothing to hide behind in the other.
///
/// It folds exactly two of the history's three families: what callers asked for
/// and what the scheduler decided. It never folds what a caller concluded. A
/// conclusion it copied would be a conclusion it could not contradict.
///
/// The log is unbounded, which is affordable only because this type is an
/// oracle and never runs a service.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReferenceScheduler {
    config: SchedulerConfig,
    log: Vec<HistoryEvent>,
}

impl ReferenceScheduler {
    /// Creates a reference scheduler with an empty history.
    #[must_use]
    pub const fn new(config: SchedulerConfig) -> Self {
        Self {
            config,
            log: Vec::new(),
        }
    }

    /// Appends one observed event.
    pub fn observe(&mut self, event: HistoryEvent) {
        self.log.push(event);
    }

    /// Appends a recorded history.
    pub fn observe_all<I: IntoIterator<Item = HistoryEvent>>(&mut self, events: I) {
        self.log.extend(events);
    }

    /// Returns the recorded history in order.
    #[must_use]
    pub fn history(&self) -> &[HistoryEvent] {
        &self.log
    }

    /// Returns the number of recorded events.
    #[must_use]
    pub fn len(&self) -> usize {
        self.log.len()
    }

    /// Returns whether nothing has been recorded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.log.is_empty()
    }

    /// Folds the whole history into everything it implies.
    #[must_use]
    pub fn replay(&self) -> Replay {
        let mut fold = Fold::new(self.config);
        for event in &self.log {
            fold.absorb(event);
        }
        fold.finish()
    }

    /// Folds the history and returns only the state it implies.
    #[must_use]
    pub fn view(&self) -> SchedulerView {
        self.replay().view
    }

    /// Folds the history and returns only the aggregate counts it implies.
    #[must_use]
    pub fn summary(&self) -> SchedulerSummary {
        self.replay().summary
    }

    /// Folds the history and judges the scheduling decisions in it.
    ///
    /// # Errors
    ///
    /// Returns the first structural violation, or the widest opportunity gap
    /// when the decisions were structurally sound but unfair.
    pub fn audit(&self) -> Result<FairnessReport, SchedulingViolation> {
        self.replay().fairness
    }
}

/// One client's derived deduplication state within one group.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DerivedSession {
    client_id: ClientId,
    epoch: SessionEpoch,
    outstanding: Option<(Sequence, CounterCommand, WorkId)>,
    completed: Option<(Sequence, CounterCommand, CounterResult)>,
}

/// One group as the history implies it.
///
/// The queue is one flat list in arrival order. The priority head is found by
/// scanning it, never by keeping a structure that already knows the answer.
///
/// There is deliberately no occupancy field here. Whether a group is holding a
/// worker is not a property of the group at all — it is a consequence of a
/// dispatch and the cost that dispatch derived — so it lives in [`Fold`]'s
/// occupancy table, keyed by group ID, where it survives the slot being
/// removed and reopened exactly as the physical worker does.
#[derive(Clone, Debug, Eq, PartialEq)]
struct DerivedGroup {
    incarnation: GroupIncarnation,
    state: GroupLifecycle,
    poisoned: bool,
    stalled: bool,
    /// Whether an availability report returned the group to the ready set at
    /// an instant when a plan could have named it, and no plan has named it
    /// since.
    ///
    /// This is what stops a stall from being revoked at exactly the instants
    /// readiness is sampled. See [`Fold::is_owed`].
    restored: bool,
    counter: i64,
    quota: WorkQuota,
    queue: Vec<(WorkId, Work)>,
    sessions: Vec<DerivedSession>,
}

impl DerivedGroup {
    fn new(incarnation: GroupIncarnation, quota: WorkQuota) -> Self {
        Self {
            incarnation,
            state: GroupLifecycle::Creating,
            poisoned: false,
            stalled: false,
            restored: false,
            counter: 0,
            quota,
            queue: Vec::new(),
            sessions: Vec::new(),
        }
    }

    /// Drops everything a removed slot must not keep.
    ///
    /// A worker the departing incarnation is still occupying is not among
    /// them, and it is not cleared here because it was never held here: an
    /// occupancy belongs to the group ID in [`Fold`], so a slot reopened
    /// before its predecessor's cost is paid stays out of the ready set until
    /// that cost is paid.
    fn clear(&mut self) {
        self.counter = 0;
        self.sessions.clear();
        self.queue.clear();
        self.poisoned = false;
        self.stalled = false;
        self.restored = false;
    }

    /// Returns whether everything the group owns *except* its reported
    /// availability admits a turn.
    ///
    /// The split exists because the stall bit is the one readiness condition
    /// the history reports rather than implies, so the audit has to be able to
    /// ask what would be true without it.
    fn is_dispatchable_but_for_availability(&self) -> bool {
        matches!(
            self.state,
            GroupLifecycle::Recovering | GroupLifecycle::Serving | GroupLifecycle::Draining
        ) && !self.poisoned
            && !self.queue.is_empty()
    }

    /// Returns whether everything about the group *itself* admits a turn.
    ///
    /// This is readiness minus the one condition a group does not own: the
    /// worker it may be occupying. [`Fold::is_ready`] adds that condition, and
    /// derives it rather than reading it off a reported flag.
    fn is_dispatchable(&self) -> bool {
        self.is_dispatchable_but_for_availability() && !self.stalled
    }

    fn admits(&self, class: WorkClass) -> bool {
        match self.state {
            GroupLifecycle::Serving => true,
            GroupLifecycle::Recovering => !matches!(class, WorkClass::Command),
            GroupLifecycle::Creating
            | GroupLifecycle::Draining
            | GroupLifecycle::Removed
            | GroupLifecycle::Tombstoned => false,
        }
    }

    /// Returns the position of the item a correct opportunity would take next,
    /// skipping positions already taken by this same opportunity.
    fn head(&self, taken: &[usize]) -> Option<usize> {
        self.queue
            .iter()
            .enumerate()
            .filter(|(index, _)| !taken.contains(index))
            .min_by_key(|(index, (_, work))| (work.class(), *index))
            .map(|(index, _)| index)
    }

    /// Returns the exact items an opportunity is offered against, in order.
    ///
    /// These are the items a correct turn services, and they carry no prices,
    /// because the fold does not price a turn from them. A turn is priced by
    /// the services it actually recorded — see [`Fold::settle`] — and this list
    /// is what those services are checked against. Pricing the turn from here
    /// instead is exactly the mistake that let a dispatch buy a full occupancy
    /// while servicing nothing.
    fn expected_dispatch(&self) -> Vec<WorkId> {
        let mut taken: Vec<usize> = Vec::new();
        let mut expected: Vec<WorkId> = Vec::new();
        for _ in 0..self.quota.get() {
            let Some(index) = self.head(&taken) else {
                break;
            };
            taken.push(index);
            let (id, work) = self.queue[index];
            expected.push(id);
            if matches!(work, Work::Faulty { .. }) {
                break;
            }
        }
        expected
    }

    fn session(&self, client_id: ClientId) -> Option<&DerivedSession> {
        self.sessions
            .iter()
            .find(|session| session.client_id == client_id)
    }
}

/// The plan a pass is still working through.
///
/// `pending` and `offered` partition the plan: a group starts in the first and
/// moves to the second when it takes its turn. The bound is that `pending` is
/// empty by the time the pass retires, and the two sets are how the fold says
/// so at any scale.
#[derive(Clone, Debug, Eq, PartialEq)]
struct OpenPass {
    pass: PassIndex,
    pending: BTreeSet<GroupId>,
    offered: BTreeSet<GroupId>,
}

impl OpenPass {
    fn planned(&self, group: GroupId) -> bool {
        self.pending.contains(&group) || self.offered.contains(&group)
    }
}

/// One turn while its work is still being recorded.
///
/// A dispatch does not carry its own price. It carries a *claim* about one,
/// and the items that back the claim arrive afterwards as
/// [`HistoryEvent::WorkServiced`] events. So a turn stays open until the
/// history moves on to something else, and only then can it be priced.
///
/// `owed` is what the turn was offered against — the head of the group's queue,
/// recomputed here — and `serviced` is what the recorded services actually
/// moved. Nothing forces the two to match, which is the whole point: the fold
/// compares them and reports the difference rather than assuming it away.
///
/// **A turn ends at the first recorded event that is not one of its own
/// services.** CONTRACT.md's "work is applied at dispatch" is what makes that
/// exact: a turn is one indivisible act, so its services follow its dispatch
/// with nothing between them, and anything else settles it. Without a
/// settlement point a turn could be left open forever and never priced at all
/// — which is what the single dispatch slot this replaced did to every turn but
/// the last in each pass.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Turn {
    pass: PassIndex,
    tick: TickIndex,
    group: GroupId,
    owed: VecDeque<WorkId>,
    owed_total: u32,
    serviced: Vec<(WorkId, u64)>,
    reported_serviced: u32,
    reported_cost: u64,
}

/// One worker a dispatch is holding, and the instant it is due back.
///
/// The oracle keeps this instead of believing a reported occupancy flag. An
/// occupancy is opened by a turn that has been *settled*, priced at exactly
/// what that turn's recorded services were worth; the release is due at
/// `dispatch tick + that price`; and the scheduler authors neither number.
///
/// There is one entry per group and there are as many entries as there are
/// live occupancies, because the pool is many workers rather than one. A turn
/// still being recorded holds a worker too, and [`Fold::occupied`] counts it —
/// it just has no deadline yet, because its price is not yet known.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Occupancy {
    pass: PassIndex,
    due: TickIndex,
}

/// How long one group has gone without a turn it was owed.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct Gap {
    run: u32,
    run_from: Option<PassIndex>,
    worst: u32,
    worst_from: Option<PassIndex>,
}

impl Gap {
    fn deny(&mut self, pass: PassIndex) {
        if self.run == 0 {
            self.run_from = Some(pass);
        }
        self.run += 1;
        if self.run > self.worst {
            self.worst = self.run;
            self.worst_from = self.run_from;
        }
    }

    const fn satisfy(&mut self) {
        self.run = 0;
        self.run_from = None;
    }
}

/// One traversal of a history, and everything that history implies.
///
/// The fold *is* the oracle. It is built fresh for every replay and dropped
/// afterwards, so nothing it derives can survive into the next question asked
/// of the same log — a replay is always a function of the events alone. Each
/// field is either a re-derivation of something [`crate::ManagedScheduler`]
/// also tracks (the groups, the queues, the counts) or an accounting of the
/// scheduling decisions themselves, which the scheduler keeps none of.
///
/// `violation` holds the *first* structural fault and ignores later ones. A
/// history that broke one rule usually goes on to break several more as a
/// consequence, and reporting the first is what makes the answer name a cause
/// instead of its aftermath.
struct Fold {
    config: SchedulerConfig,
    groups: BTreeMap<GroupId, DerivedGroup>,
    next_work: u64,
    admitted: u64,
    serviced: u64,
    failed: u64,
    queued: u32,
    outcomes: Vec<OperationOutcome>,
    services: Vec<ServiceRecord>,
    failures: Vec<FailureRecord>,
    open: Option<OpenPass>,
    next_pass: PassIndex,
    passes_armed: u64,
    passes_completed: u64,
    opportunities: u64,
    widest_plan: u32,
    gaps: BTreeMap<GroupId, Gap>,
    /// Workers held by settled turns, and the tick each is due back at.
    occupancies: BTreeMap<GroupId, Occupancy>,
    /// The turn whose services are still arriving, if any.
    recording: Option<Turn>,
    /// Highest tick any recorded decision has claimed.
    tick: TickIndex,
    /// Ticks the last plan was armed at and the last plan retired at.
    last_armed: Option<TickIndex>,
    last_retired: Option<TickIndex>,
    violation: Option<(FaultSite, SchedulingViolation)>,
}

impl Fold {
    fn new(config: SchedulerConfig) -> Self {
        Self {
            config,
            groups: BTreeMap::new(),
            next_work: 1,
            admitted: 0,
            serviced: 0,
            failed: 0,
            queued: 0,
            outcomes: Vec::new(),
            services: Vec::new(),
            failures: Vec::new(),
            open: None,
            next_pass: PassIndex::first(),
            passes_armed: 0,
            passes_completed: 0,
            opportunities: 0,
            widest_plan: 0,
            gaps: BTreeMap::new(),
            occupancies: BTreeMap::new(),
            recording: None,
            tick: TickIndex::ZERO,
            last_armed: None,
            last_retired: None,
            violation: None,
        }
    }

    fn absorb(&mut self, event: &HistoryEvent) {
        // Work is applied at dispatch, so a turn and its services are one act.
        // Anything else ends the turn and is the instant it can be priced.
        if !self.continues_recorded_turn(event) {
            self.settle();
        }
        match event {
            HistoryEvent::Invoked { operation, .. } => {
                let outcome = self.invoke(*operation);
                self.outcomes.push(outcome);
            }
            HistoryEvent::AvailabilityReported {
                tick,
                group,
                availability,
            } => {
                self.reach(*tick);
                self.report_availability(*group, *availability);
            }
            // The release is judged against the occupancy it claims to end
            // before the clock moves on, so a release that arrives exactly when
            // it is due is not first mistaken for one that never came.
            HistoryEvent::WorkerReleased { tick, group } => {
                if self.monotone(*tick) {
                    self.release(*tick, *group);
                    self.reach(*tick);
                }
            }
            HistoryEvent::PassArmed { pass, tick, plan } => {
                self.reach(*tick);
                self.arm(*pass, *tick, plan);
            }
            HistoryEvent::GroupOffered {
                pass,
                tick,
                group,
                outcome,
            } => {
                self.reach(*tick);
                self.offer(*pass, *tick, *group, *outcome);
            }
            HistoryEvent::WorkServiced { pass, group, work } => self.service(*pass, *group, *work),
            HistoryEvent::PassCompleted { pass, tick } => {
                self.reach(*tick);
                self.complete(*pass, *tick);
            }
            // Conclusions a caller drew are deliberately not folded.
            HistoryEvent::Completed { .. }
            | HistoryEvent::Unknown { .. }
            | HistoryEvent::NotAdmitted { .. } => {}
        }
    }

    /// Returns whether an event is part of the turn currently being recorded.
    ///
    /// Only that turn's own services are, and observations — which the fold
    /// never folds — cannot end anything because they are not decisions. A
    /// service naming another group belongs to another turn, so it settles this
    /// one first and is then judged against a turn that is no longer open.
    fn continues_recorded_turn(&self, event: &HistoryEvent) -> bool {
        match event {
            HistoryEvent::WorkServiced { group, .. } => self
                .recording
                .as_ref()
                .is_some_and(|turn| turn.group == *group),
            HistoryEvent::Completed { .. }
            | HistoryEvent::Unknown { .. }
            | HistoryEvent::NotAdmitted { .. } => true,
            HistoryEvent::Invoked { .. }
            | HistoryEvent::AvailabilityReported { .. }
            | HistoryEvent::WorkerReleased { .. }
            | HistoryEvent::PassArmed { .. }
            | HistoryEvent::GroupOffered { .. }
            | HistoryEvent::PassCompleted { .. } => false,
        }
    }

    /// Records an external availability report, and decides whether it puts the
    /// group back in the fairness question.
    ///
    /// # Scope
    ///
    /// This opens a debt at exactly one kind of instant, and the direction the
    /// rest of the fold reads it in is [`Self::is_owed`] at each arming:
    ///
    /// > For every recorded `Available` report naming a group that is, **at
    /// > that instant**, stalled, dispatchable but for its availability,
    /// > holding no worker, and not waiting behind a pass the host cannot
    /// > finish: the group is owed the next plan armed, and the debt is
    /// > discharged by a plan naming it and by nothing else. A `Stalled` report
    /// > arriving first does not retract it.
    ///
    /// Where the report falls in the *pass cycle* does not enter that, and it
    /// used to. `restorable` required `self.open.is_none()`, on the stated
    /// ground that "a stall raised while a pass was already open still excuses,
    /// because no plan could have been armed to name the group". That is a
    /// claim about the instant a stall is *raised*, and it was applied to the
    /// instant availability is *restored* — a different instant, an open pass
    /// apart. The scheduler authors both the availability reports and the pass
    /// boundaries, so it chose the window: available for the whole interior of
    /// every pass, stalled for each arming, never sampled ready, never owed,
    /// and starved at a `widest_gap` of zero over twenty passes. What replaces
    /// it is not "a pass was open" but [`Self::exhausted_mid_traversal`] — a
    /// pass with more of its plan left than the pool has workers to reach it
    /// with — which is the bound's own "absent global resource exhaustion"
    /// precondition rather than a fact the scheduler can manufacture by
    /// dispatching one group and waiting.
    ///
    /// # Outside this scope
    ///
    /// Three qualifications remain. The first two deny a group at most one
    /// window each, because a scheduler cannot re-establish either without
    /// giving the group up or serving it; the third is unbounded and is the
    /// known limit of the rule rather than a claim about it. Each has a
    /// boundary test in `tests/redteam_occupancy.rs`.
    ///
    /// - **Restored while the group holds a worker.** No debt: the worker was
    ///   what kept it out, and that worker's price is work the group actually
    ///   did. To repeat the dodge the group must be occupied again, which takes
    ///   a dispatch, which takes a plan naming it. See
    ///   `restoring_availability_inside_an_occupancy_denies_one_window_and_no_more`.
    /// - **Restored while the group holds no work, is poisoned, or is not in a
    ///   serviceable lifecycle state.** No debt: the stall was not what kept it
    ///   out. To repeat the dodge the queue must empty again, which takes
    ///   service. See
    ///   `restoring_availability_with_an_empty_queue_denies_one_window_and_no_more`.
    /// - **Restored while the open pass cannot be finished.** No debt, and this
    ///   one repeats: a host that keeps more plan entries pending than it has
    ///   workers, and lets a group's availability appear only there, is owed
    ///   nothing however long that runs. It is not distinguishable at the level
    ///   of recorded decisions from a host that is simply busy, and the entries
    ///   it holds pending are ready groups it must still offer. See
    ///   `a_pass_the_host_cannot_finish_excuses_the_availability_it_spans`, and
    ///   CONTRACT.md, "What the audit does not claim".
    ///
    /// What the rule does not touch at all is a stall that holds: a group
    /// reported `Stalled` and never reported available again is owed nothing,
    /// however long it holds work. See
    /// `a_stall_held_unbroken_is_the_one_legitimate_way_a_group_receives_nothing`.
    fn report_availability(&mut self, group: GroupId, availability: GroupAvailability) {
        let restorable = !self.exhausted_mid_traversal()
            && !self.occupied(group)
            && self
                .groups
                .get(&group)
                .is_some_and(|state| state.stalled && state.is_dispatchable_but_for_availability());
        let Some(state) = self.groups.get_mut(&group) else {
            return;
        };
        match availability {
            GroupAvailability::Stalled => state.stalled = true,
            GroupAvailability::Available => {
                state.stalled = false;
                state.restored |= restorable;
            }
        }
    }

    /// Returns whether the open pass has more of its plan left to traverse than
    /// the pool has free workers to traverse it with.
    ///
    /// This is the one state in which the scheduler could not have reached an
    /// arming, and it is the required bound's own precondition — "**absent
    /// global resource exhaustion**, every continuously ready group receives a
    /// scheduling opportunity within one complete pass" — made checkable from
    /// facts the fold derives rather than reads. No plan may be armed while one
    /// is open, and a plan may not retire owing a turn, so a pass it cannot
    /// finish is a pass that stands between the scheduler and the next arming.
    ///
    /// Both terms are load bearing:
    ///
    /// - **Entries still pending.** A pass with nothing pending has finished
    ///   traversing and is merely being *held* open. The bound's proof — "a
    ///   group that becomes ready part way through a pass is in the next one,
    ///   so it waits at most one complete pass" — assumes a traversal in
    ///   progress, and knows no such pass. Retiring it and arming one that
    ///   names the group was available at that instant.
    /// - **More pending than free workers.** Arming is free even under
    ///   exhaustion — a plan armed with no workers simply suspends — so
    ///   exhaustion excuses nothing on its own; what it excuses is the *pass*
    ///   it is holding open. A pass whose remaining entries all have a worker
    ///   waiting for them could have been finished, retired, and replaced.
    ///   Counting free workers rather than held ones matters: a host is at its
    ///   emptiest at the instant a report lands, because every occupancy that
    ///   came due has already been called in and the tick's dispatches have not
    ///   yet been taken. `workers_held` alone reads zero there for a host with
    ///   thousands of groups pending on thirty-two workers.
    fn exhausted_mid_traversal(&self) -> bool {
        let free = usize::try_from(self.config.workers().saturating_sub(self.workers_held()))
            .unwrap_or(usize::MAX);
        self.open
            .as_ref()
            .is_some_and(|open| open.pending.len() > free)
    }

    /// Reports whether a recorded tick may be believed, and faults when it
    /// walks the fold's clock backwards.
    fn monotone(&mut self, tick: TickIndex) -> bool {
        if tick < self.tick {
            self.fault(
                FaultSite::ClockWalkedBackwards,
                SchedulingViolation::TickWentBackwards {
                    current: self.tick,
                    observed: tick,
                },
            );
            return false;
        }
        true
    }

    /// Advances the fold's clock to a recorded tick and calls in every worker
    /// occupancy whose cost was paid before it.
    ///
    /// An occupancy due at exactly this tick is not overdue: that is the tick
    /// its release belongs to. One due earlier has outlived its cost, and the
    /// fold stops counting it as an occupancy at that point — so the group it
    /// was holding rejoins the ready set and starts accruing gap, rather than
    /// disappearing from the fairness question entirely.
    fn reach(&mut self, tick: TickIndex) {
        if !self.monotone(tick) {
            return;
        }
        self.tick = tick;
        let overdue: Vec<(GroupId, Occupancy)> = self
            .occupancies
            .iter()
            .filter(|(_, occupancy)| occupancy.due < tick)
            .map(|(group, occupancy)| (*group, *occupancy))
            .collect();
        for (group, occupancy) in overdue {
            self.occupancies.remove(&group);
            self.fault(
                FaultSite::OccupancyOutlivedItsCost,
                SchedulingViolation::WorkerHeldPastCost {
                    pass: occupancy.pass,
                    group,
                    due: occupancy.due,
                    observed: tick,
                },
            );
        }
    }

    /// Ends one worker occupancy, and judges the release against the cost that
    /// opened it.
    fn release(&mut self, tick: TickIndex, group: GroupId) {
        let Some(occupancy) = self.occupancies.remove(&group) else {
            self.fault(
                FaultSite::ReleaseWithoutOccupancy,
                SchedulingViolation::SpuriousWorkerRelease { tick, group },
            );
            return;
        };
        match tick.cmp(&occupancy.due) {
            Ordering::Less => self.fault(
                FaultSite::ReleaseBeforeDue,
                SchedulingViolation::WorkerReleasedEarly {
                    pass: occupancy.pass,
                    group,
                    due: occupancy.due,
                    observed: tick,
                },
            ),
            Ordering::Greater => self.fault(
                FaultSite::ReleaseAfterDue,
                SchedulingViolation::WorkerHeldPastCost {
                    pass: occupancy.pass,
                    group,
                    due: occupancy.due,
                    observed: tick,
                },
            ),
            Ordering::Equal => {}
        }
    }

    /// Returns whether a group is holding a worker whose cost is unpaid.
    ///
    /// A worker is held either by a turn still being recorded — which holds one
    /// by definition, and whose price is not yet known — or by a settled
    /// occupancy that has not reached its deadline.
    fn occupied(&self, group: GroupId) -> bool {
        self.recording
            .as_ref()
            .is_some_and(|turn| turn.group == group)
            || self
                .occupancies
                .get(&group)
                .is_some_and(|occupancy| occupancy.due > self.tick)
    }

    /// Returns how many workers of the pool are held right now.
    fn workers_held(&self) -> u32 {
        // A group cannot appear in both terms: a group with a live occupancy is
        // not ready, so it cannot have been dispatched into a turn.
        let settled = self
            .occupancies
            .values()
            .filter(|occupancy| occupancy.due > self.tick)
            .count();
        u32::try_from(settled + usize::from(self.recording.is_some())).unwrap_or(u32::MAX)
    }

    /// Returns whether a group would take a turn if it were offered one.
    ///
    /// Every one of the five conditions is derived. Four are the group's own
    /// state, and the fifth — that it is not occupying a worker — is the one
    /// the scheduler used to assert and the fold now computes, because a
    /// condition the audited party defines is not a condition.
    fn is_ready(&self, group: GroupId) -> bool {
        self.groups
            .get(&group)
            .is_some_and(DerivedGroup::is_dispatchable)
            && !self.occupied(group)
    }

    /// Returns whether a plan armed now owes this group a turn.
    ///
    /// This is [`Self::is_ready`] with one deliberate difference, and it is the
    /// difference between a stall that held and a stall that moved. A group
    /// whose stall was broken at an instant when a plan could have named it is
    /// owed the next plan whatever its availability by the time that plan is
    /// armed, because a signal that revokes readiness only at the instants
    /// readiness is sampled has not kept the group out of anything — it has
    /// merely kept it out of the question.
    ///
    /// A group owed a plan it does not appear in accrues gap. It is still not
    /// *ready*, so a plan that named it would break plan totality; the only way
    /// to settle the debt is to stop breaking the stall.
    fn is_owed(&self, group: GroupId) -> bool {
        self.groups.get(&group).is_some_and(|state| {
            state.is_dispatchable_but_for_availability() && (!state.stalled || state.restored)
        }) && !self.occupied(group)
    }

    fn invoke(&mut self, operation: Operation) -> OperationOutcome {
        match operation {
            Operation::Lifecycle { group, request } => {
                OperationOutcome::Lifecycle(self.lifecycle(group, request))
            }
            Operation::OpenSession {
                group,
                incarnation,
                client_id,
                session_epoch,
            } => OperationOutcome::Session(self.open_session(
                group,
                incarnation,
                client_id,
                session_epoch,
            )),
            Operation::Submit {
                group,
                incarnation,
                work,
            } => OperationOutcome::Admission(self.submit(group, incarnation, work)),
        }
    }

    fn lifecycle(&mut self, group: GroupId, request: LifecycleRequest) -> LifecycleOutcome {
        if !self.config.admits_group(group) {
            return LifecycleOutcome::Rejected(LifecycleRejection::GroupOutOfRange);
        }
        let target = request.target();
        let Some(state) = self.groups.get_mut(&group) else {
            let LifecycleRequest::Create { quota } = request else {
                return LifecycleOutcome::Rejected(LifecycleRejection::GroupUnknown);
            };
            self.groups
                .insert(group, DerivedGroup::new(GroupIncarnation::first(), quota));
            return LifecycleOutcome::Created {
                incarnation: GroupIncarnation::first(),
            };
        };

        if state.state == GroupLifecycle::Tombstoned {
            return if target == GroupLifecycle::Tombstoned {
                LifecycleOutcome::Idempotent {
                    state: GroupLifecycle::Tombstoned,
                    incarnation: state.incarnation,
                }
            } else {
                LifecycleOutcome::Rejected(LifecycleRejection::GroupTombstoned)
            };
        }

        if let LifecycleRequest::Create { quota } = request {
            return match state.state {
                GroupLifecycle::Creating if state.quota == quota => LifecycleOutcome::Idempotent {
                    state: GroupLifecycle::Creating,
                    incarnation: state.incarnation,
                },
                GroupLifecycle::Creating => {
                    LifecycleOutcome::Rejected(LifecycleRejection::QuotaConflict {
                        current: state.quota,
                    })
                }
                GroupLifecycle::Removed => {
                    let Some(next) = state.incarnation.successor() else {
                        return LifecycleOutcome::Rejected(
                            LifecycleRejection::IncarnationExhausted,
                        );
                    };
                    state.incarnation = next;
                    state.state = GroupLifecycle::Creating;
                    state.quota = quota;
                    state.clear();
                    LifecycleOutcome::Created { incarnation: next }
                }
                current => LifecycleOutcome::Rejected(LifecycleRejection::Conflict {
                    current,
                    requested: GroupLifecycle::Creating,
                }),
            };
        }

        if state.state == target {
            let outcome = LifecycleOutcome::Idempotent {
                state: target,
                incarnation: state.incarnation,
            };
            if target == GroupLifecycle::Draining {
                self.retire_poisoned(group);
            }
            return outcome;
        }

        let legal = match request {
            LifecycleRequest::Recover => state.state == GroupLifecycle::Creating,
            LifecycleRequest::Serve => state.state == GroupLifecycle::Recovering,
            LifecycleRequest::Drain => matches!(
                state.state,
                GroupLifecycle::Creating | GroupLifecycle::Recovering | GroupLifecycle::Serving
            ),
            LifecycleRequest::Remove => state.state == GroupLifecycle::Draining,
            LifecycleRequest::Tombstone => state.state == GroupLifecycle::Removed,
            LifecycleRequest::Create { .. } => false,
        };
        if !legal {
            return LifecycleOutcome::Rejected(LifecycleRejection::Conflict {
                current: state.state,
                requested: target,
            });
        }
        if request == LifecycleRequest::Remove && !state.queue.is_empty() {
            return LifecycleOutcome::Rejected(LifecycleRejection::QueueNotDrained {
                pending: queue_len(state),
            });
        }

        let from = state.state;
        state.state = target;
        let incarnation = state.incarnation;
        if target == GroupLifecycle::Removed {
            state.clear();
        }
        if target == GroupLifecycle::Draining {
            self.retire_poisoned(group);
        }
        LifecycleOutcome::Applied {
            from,
            to: target,
            incarnation,
        }
    }

    fn retire_poisoned(&mut self, group: GroupId) {
        let Some(state) = self.groups.get_mut(&group) else {
            return;
        };
        if !state.poisoned {
            return;
        }
        let mut order: Vec<usize> = (0..state.queue.len()).collect();
        order.sort_by_key(|index| (state.queue[*index].1.class(), *index));
        for index in order {
            self.failures.push(FailureRecord {
                work: state.queue[index].0,
                group,
                reason: WorkFailure::GroupPoisoned,
            });
        }
        let retired = queue_len(state);
        state.queue.clear();
        for session in &mut state.sessions {
            session.outstanding = None;
        }
        self.queued -= retired;
        self.failed += u64::from(retired);
    }

    fn open_session(
        &mut self,
        group: GroupId,
        incarnation: GroupIncarnation,
        client_id: ClientId,
        epoch: SessionEpoch,
    ) -> SessionOutcome {
        if let Err(rejection) = self.address(group, incarnation) {
            return SessionOutcome::Rejected(rejection);
        }
        let admits_client = self.config.admits_client(client_id);
        let Some(state) = self.groups.get_mut(&group) else {
            return SessionOutcome::Rejected(AdmissionRejection::GroupUnknown);
        };
        // Recomputed against the states that establish sessions, which are not
        // the states that admit `Command` work: recovery does the first and
        // refuses the second.
        if !matches!(
            state.state,
            GroupLifecycle::Recovering | GroupLifecycle::Serving
        ) {
            return SessionOutcome::Rejected(AdmissionRejection::GroupNotAcceptingSessions {
                state: state.state,
            });
        }
        if state.poisoned {
            return SessionOutcome::Rejected(AdmissionRejection::GroupPoisoned);
        }
        if !admits_client {
            return SessionOutcome::Rejected(AdmissionRejection::ClientOutOfRange);
        }

        let existing = state
            .sessions
            .iter()
            .position(|session| session.client_id == client_id);
        let Some(position) = existing else {
            state.sessions.push(DerivedSession {
                client_id,
                epoch,
                outstanding: None,
                completed: None,
            });
            return SessionOutcome::Opened {
                session_epoch: epoch,
            };
        };

        let current = state.sessions[position].epoch;
        if epoch < current {
            return SessionOutcome::Rejected(AdmissionRejection::StaleSession { current });
        }
        if epoch == current {
            return SessionOutcome::AlreadyOpen {
                session_epoch: epoch,
            };
        }
        state.sessions[position] = DerivedSession {
            client_id,
            epoch,
            outstanding: None,
            completed: None,
        };
        SessionOutcome::Replaced {
            session_epoch: epoch,
        }
    }

    fn submit(
        &mut self,
        group: GroupId,
        incarnation: GroupIncarnation,
        work: Work,
    ) -> AdmissionOutcome {
        if let Err(rejection) = self.address(group, incarnation) {
            return AdmissionOutcome::Rejected(rejection);
        }
        let class = work.class();
        let group_limit = self.config.max_group_queue();
        let global_limit = self.config.max_global_queue();
        let global_queued = self.queued;
        let admits_client = work
            .request_identity()
            .is_none_or(|request| self.config.admits_client(request.client_id));
        let Some(state) = self.groups.get_mut(&group) else {
            return AdmissionOutcome::Rejected(AdmissionRejection::GroupUnknown);
        };

        if !state.admits(class) {
            return AdmissionOutcome::Rejected(AdmissionRejection::GroupNotAcceptingWork {
                state: state.state,
                class,
            });
        }
        if state.poisoned {
            return AdmissionOutcome::Rejected(AdmissionRejection::GroupPoisoned);
        }

        if let Work::Counter {
            request, command, ..
        } = work
        {
            if !admits_client {
                return AdmissionOutcome::Rejected(AdmissionRejection::ClientOutOfRange);
            }
            let Some(session) = state.session(request.client_id) else {
                return AdmissionOutcome::Rejected(AdmissionRejection::SessionNotOpen);
            };
            if let Some(answer) = session_verdict(session, request, command) {
                return answer;
            }
        }

        if queue_len(state) >= group_limit {
            return AdmissionOutcome::Rejected(AdmissionRejection::GroupQueueFull {
                limit: group_limit,
            });
        }
        if global_queued >= global_limit {
            return AdmissionOutcome::Rejected(AdmissionRejection::GlobalQueueFull {
                limit: global_limit,
            });
        }

        let id = WorkId::new(self.next_work).expect("work identifiers start at one");
        state.queue.push((id, work));
        if let Work::Counter {
            request, command, ..
        } = work
        {
            if let Some(session) = state
                .sessions
                .iter_mut()
                .find(|session| session.client_id == request.client_id)
            {
                session.outstanding = Some((request.sequence, command, id));
            }
        }
        self.next_work += 1;
        self.queued += 1;
        self.admitted += 1;
        AdmissionOutcome::Queued { work: id }
    }

    /// Recomputes the gates that depend only on the addressed slot's identity:
    /// whether it is configured, exists, has been tombstoned, and is the
    /// incarnation the traffic named. Every later gate needs a live slot, so
    /// these run first and answer for both submissions and session opens.
    fn address(
        &self,
        group: GroupId,
        incarnation: GroupIncarnation,
    ) -> Result<(), AdmissionRejection> {
        if !self.config.admits_group(group) {
            return Err(AdmissionRejection::GroupOutOfRange);
        }
        let Some(state) = self.groups.get(&group) else {
            return Err(AdmissionRejection::GroupUnknown);
        };
        if state.state == GroupLifecycle::Tombstoned {
            return Err(AdmissionRejection::GroupTombstoned);
        }
        if incarnation < state.incarnation {
            return Err(AdmissionRejection::StaleIncarnation {
                current: state.incarnation,
            });
        }
        if incarnation > state.incarnation {
            return Err(AdmissionRejection::FutureIncarnation {
                current: state.incarnation,
            });
        }
        Ok(())
    }

    /// Records the first structural fault, with the check that caught it.
    ///
    /// `site` is not derived from `violation` and could not be: five checks
    /// report a variant another check already reports, so the variant does not
    /// determine the check. It is passed in because the caller is the only
    /// party that knows which rule it is deciding.
    fn fault(&mut self, site: FaultSite, violation: SchedulingViolation) {
        if self.violation.is_none() {
            self.violation = Some((site, violation));
        }
    }

    /// Recomputes the ready set from first principles and judges the plan
    /// against it.
    fn arm(&mut self, pass: PassIndex, tick: TickIndex, plan: &[GroupId]) {
        if let Some(open) = self.open.as_ref().map(|open| open.pass) {
            self.fault(
                FaultSite::ArmedOverAnOpenPass,
                SchedulingViolation::PassArmedWhileOpen { open, armed: pass },
            );
        }
        if pass != self.next_pass {
            self.fault(
                FaultSite::ArmedOutOfOrder,
                SchedulingViolation::PassOutOfOrder {
                    expected: self.next_pass,
                    observed: pass,
                },
            );
        }
        // A tick arms at most one plan. Without this, a scheduler could arm
        // plans without limit at a standstill clock, denying a group every one
        // of them while no occupancy it holds ever came due.
        if self.last_armed.is_some_and(|last| tick <= last) {
            self.fault(
                FaultSite::ArmedTickReused,
                SchedulingViolation::PassBoundaryReused { pass, tick },
            );
        }
        self.last_armed = Some(tick);
        self.next_pass = pass.successor().unwrap_or(pass);
        self.passes_armed += 1;
        self.widest_plan = self
            .widest_plan
            .max(u32::try_from(plan.len()).unwrap_or(u32::MAX));

        let mut seen: BTreeSet<GroupId> = BTreeSet::new();
        for group in plan {
            if !seen.insert(*group) {
                self.fault(
                    FaultSite::PlanRepeatedAGroup,
                    SchedulingViolation::PlanRepeatedGroup {
                        pass,
                        group: *group,
                    },
                );
            }
            if !self.is_ready(*group) {
                self.fault(
                    FaultSite::PlanNamedAnUnreadyGroup,
                    SchedulingViolation::PlanIncludedUnreadyGroup {
                        pass,
                        group: *group,
                    },
                );
            }
        }

        // Every group this plan owes a turn and did not name has been denied an
        // opportunity, and the run of consecutive denials is the gap the bound
        // forbids. A group that is owed nothing resets its run rather than
        // accumulating while it has no work to do.
        //
        // Being owed is the derived thing, twice over. A group whose worker
        // occupancy has outlived its cost is ready again and is owed this
        // plan's turn like any other, which is what stops an unreported release
        // from converting starvation into silence; and a group whose stall was
        // broken since a plan could last have named it is owed this one too,
        // which is what stops the stall bit from doing the same.
        let groups: Vec<GroupId> = self.groups.keys().copied().collect();
        for group in groups {
            let denied = self.is_owed(group) && !seen.contains(&group);
            if denied {
                self.gaps.entry(group).or_default().deny(pass);
                continue;
            }
            self.gaps.entry(group).or_default().satisfy();
            // The debt a broken stall opened is settled by this plan: either it
            // named the group, or the group was owed nothing anyway.
            if let Some(state) = self.groups.get_mut(&group) {
                state.restored = false;
            }
        }

        self.open = Some(OpenPass {
            pass,
            pending: seen,
            offered: BTreeSet::new(),
        });
    }

    fn offer(&mut self, pass: PassIndex, tick: TickIndex, group: GroupId, outcome: OfferOutcome) {
        self.opportunities += 1;
        let planned = self
            .open
            .as_ref()
            .is_some_and(|open| open.pass == pass && open.planned(group));
        if !planned {
            self.fault(
                FaultSite::OfferOutsideThePlan,
                SchedulingViolation::OfferOutsidePlan { pass, group },
            );
            return;
        }
        if self
            .open
            .as_ref()
            .is_some_and(|open| open.offered.contains(&group))
        {
            self.fault(
                FaultSite::OfferedTwiceInOnePass,
                SchedulingViolation::GroupOfferedTwice { pass, group },
            );
            return;
        }
        if let Some(open) = self.open.as_mut() {
            open.pending.remove(&group);
            open.offered.insert(group);
        }

        let Some(state) = self.groups.get(&group) else {
            self.fault(
                FaultSite::OfferedAnUnknownGroup,
                SchedulingViolation::DispatchedUnreadyGroup { pass, group },
            );
            return;
        };
        let stalled = state.stalled;
        let quota = state.quota.get();
        let owed = state.expected_dispatch();
        let ready = self.is_ready(group);

        match outcome {
            OfferOutcome::Skipped(SkipReason::Stalled) => {
                if !stalled {
                    self.fault(
                        FaultSite::SkippedWhileAvailable,
                        SchedulingViolation::SkippedAvailableGroup { pass, group },
                    );
                }
            }
            OfferOutcome::Dispatched { serviced, cost } => {
                if !ready {
                    self.fault(
                        FaultSite::DispatchedWhileUnready,
                        SchedulingViolation::DispatchedUnreadyGroup { pass, group },
                    );
                    return;
                }
                if serviced > quota {
                    self.fault(
                        FaultSite::DispatchOverQuota,
                        SchedulingViolation::QuotaExceeded {
                            pass,
                            group,
                            serviced,
                            quota,
                        },
                    );
                }
                // Nothing about the turn's price is decided here. The dispatch
                // has only made a claim; the work that backs it arrives next,
                // and the turn is judged and priced when it ends.
                self.recording = Some(Turn {
                    pass,
                    tick,
                    group,
                    owed_total: u32::try_from(owed.len()).unwrap_or(u32::MAX),
                    owed: owed.into_iter().collect(),
                    serviced: Vec::new(),
                    reported_serviced: serviced,
                    reported_cost: cost,
                });
                // A turn in progress holds a worker, so the pool is checked the
                // instant the turn opens rather than when it is priced.
                if self.workers_held() > self.config.workers() {
                    self.fault(
                        FaultSite::DispatchOverWorkerPool,
                        SchedulingViolation::WorkerCountExceeded {
                            pass,
                            group,
                            workers: self.config.workers(),
                        },
                    );
                }
            }
        }
    }

    /// Ends the turn whose services were still arriving, and prices it.
    ///
    /// This is where occupancy stops being a claim and becomes a derivation.
    /// The turn is compared against the queue it was offered against and
    /// against the count and cost its dispatch reported, and the occupancy it
    /// bought is opened at what its *recorded services* were worth — not at
    /// what it was offered, and not at what it said.
    ///
    /// A deadline beyond the tick ceiling saturates rather than wrapping. The
    /// oracle judges histories it did not produce, so an arithmetic edge in an
    /// adversarial one must never be the thing that manufactures a fault.
    fn settle(&mut self) {
        let Some(turn) = self.recording.take() else {
            return;
        };
        let serviced = u32::try_from(turn.serviced.len()).unwrap_or(u32::MAX);
        // The items at the head of the queue are what the turn was for. Ending
        // it with any of them still queued means the worker was held for work
        // that never moved.
        if !turn.owed.is_empty() {
            self.fault(
                FaultSite::TurnLeftWorkQueued,
                SchedulingViolation::DispatchLeftWorkUnserviced {
                    pass: turn.pass,
                    group: turn.group,
                    owed: turn.owed_total,
                    serviced,
                },
            );
        }
        if serviced != turn.reported_serviced {
            self.fault(
                FaultSite::TurnMiscountedItsWork,
                SchedulingViolation::ServiceCountMismatch {
                    pass: turn.pass,
                    group: turn.group,
                    expected: serviced,
                    observed: turn.reported_serviced,
                },
            );
        }
        let price: u64 = turn.serviced.iter().map(|(_, cost)| *cost).sum();
        if price != turn.reported_cost {
            self.fault(
                FaultSite::TurnMispricedItsWork,
                SchedulingViolation::DispatchCostMismatch {
                    pass: turn.pass,
                    group: turn.group,
                    expected: price,
                    observed: turn.reported_cost,
                },
            );
        }
        let due = TickIndex::new(turn.tick.get().saturating_add(price));
        self.occupancies.insert(
            turn.group,
            Occupancy {
                pass: turn.pass,
                due,
            },
        );
    }

    fn service(&mut self, pass: PassIndex, group: GroupId, work: WorkId) {
        // A service that belongs to no open turn — or to one in another pass —
        // is work the history claims outside any dispatch that could have done
        // it. The group check is already spent: a service naming another group
        // settled this turn before reaching here.
        let matches_turn = self
            .recording
            .as_ref()
            .is_some_and(|turn| turn.group == group && turn.pass == pass);
        if !matches_turn {
            self.fault(
                FaultSite::ServiceOutsideATurn,
                SchedulingViolation::ServiceOutsideDispatch { pass, group },
            );
            return;
        }
        let owed = self
            .recording
            .as_mut()
            .and_then(|turn| turn.owed.pop_front());
        let Some(owed) = owed else {
            let owed_total = self.recording.as_ref().map_or(0, |turn| turn.owed_total);
            self.fault(
                FaultSite::ServicePastTheTurnsWork,
                SchedulingViolation::DispatchServicedBeyondItsWork {
                    pass,
                    group,
                    owed: owed_total,
                    work,
                },
            );
            return;
        };
        if owed != work {
            self.fault(
                FaultSite::ServiceOutOfOrder,
                SchedulingViolation::ServiceOrderViolation {
                    pass,
                    group,
                    expected: owed,
                    observed: work,
                },
            );
        }
        // The turn is priced by what it moved, so the price is read off the
        // item the service actually named. An item the group never held is
        // worth nothing, and has already been reported as the wrong item.
        let price = self
            .groups
            .get(&group)
            .and_then(|state| state.queue.iter().find(|(id, _)| *id == work))
            .map_or(0, |(_, item)| u64::from(item.cost().get()));
        if let Some(turn) = self.recording.as_mut() {
            turn.serviced.push((work, price));
        }
        self.apply(group, work);
    }

    /// Recomputes what servicing one item does to a group.
    fn apply(&mut self, group: GroupId, work: WorkId) {
        let Some(state) = self.groups.get_mut(&group) else {
            return;
        };
        let Some(position) = state.queue.iter().position(|(id, _)| *id == work) else {
            return;
        };
        let (_, item) = state.queue.remove(position);
        self.queued -= 1;
        self.serviced += 1;

        let result = match item {
            Work::System { .. } => None,
            Work::Faulty { .. } => {
                state.poisoned = true;
                None
            }
            Work::Counter {
                request, command, ..
            } => {
                let outcome = match command {
                    CounterCommand::Add { delta } => match state.counter.checked_add(delta.get()) {
                        Some(value) => {
                            state.counter = value;
                            CounterResult::Added { value }
                        }
                        None => CounterResult::Rejected(CounterRejection::CounterOverflow {
                            current: state.counter,
                        }),
                    },
                    CounterCommand::Read => CounterResult::Value {
                        value: state.counter,
                    },
                };
                if let Some(session) = state
                    .sessions
                    .iter_mut()
                    .find(|session| session.client_id == request.client_id)
                {
                    if session.epoch == request.session_epoch
                        && session
                            .outstanding
                            .is_some_and(|(_, _, queued)| queued == work)
                    {
                        session.outstanding = None;
                        session.completed = Some((request.sequence, command, outcome));
                    }
                }
                Some(outcome)
            }
        };

        self.services.push(ServiceRecord {
            work,
            group,
            class: item.class(),
            result,
        });
    }

    /// Retires a plan.
    ///
    /// The turn a pass was still recording is already settled: [`Self::absorb`]
    /// ends it at the first event that is not one of its own services, and this
    /// is one. So there is no owed queue left here to drop, which is what this
    /// method used to do to every turn in every pass.
    fn complete(&mut self, pass: PassIndex, tick: TickIndex) {
        // A tick retires at most one plan, the counterpart of the rule that it
        // arms at most one.
        if self.last_retired.is_some_and(|last| tick <= last) {
            self.fault(
                FaultSite::RetiredTickReused,
                SchedulingViolation::PassBoundaryReused { pass, tick },
            );
        }
        self.last_retired = Some(tick);
        let Some(open) = self.open.take() else {
            self.fault(
                FaultSite::RetiredWithNoPassOpen,
                SchedulingViolation::PassOutOfOrder {
                    expected: self.next_pass,
                    observed: pass,
                },
            );
            return;
        };
        if open.pass != pass {
            self.fault(
                FaultSite::RetiredADifferentPass,
                SchedulingViolation::PassOutOfOrder {
                    expected: open.pass,
                    observed: pass,
                },
            );
        }
        for group in &open.pending {
            self.fault(
                FaultSite::RetiredWithATurnOwing,
                SchedulingViolation::PassCompletedWithUnofferedGroup {
                    pass,
                    group: *group,
                },
            );
        }
        self.passes_completed += 1;
    }

    fn finish(mut self) -> Replay {
        // A history that ends inside a turn still has to price it: the end of
        // the record is as much a settlement point as any event in it.
        self.settle();
        if u64::from(self.queued) + self.serviced + self.failed != self.admitted {
            self.fault(
                FaultSite::WorkUnaccountedFor,
                SchedulingViolation::WorkNotConserved {
                    admitted: self.admitted,
                    serviced: self.serviced,
                    failed: self.failed,
                    queued: self.queued,
                },
            );
        }

        // Structural faults outrank the gap: decisions that were not a pass at
        // all cannot be judged for the fairness of a pass.
        let fault = match self.violation {
            Some(pair) => Some(pair),
            None => self
                .widest_gap()
                .map(|violation| (FaultSite::WidestOpportunityGap, violation)),
        };
        let fairness = match fault {
            Some((_, violation)) => Err(violation),
            None => Ok(FairnessReport {
                passes_armed: self.passes_armed,
                passes_completed: self.passes_completed,
                opportunities: self.opportunities,
                serviced: self.serviced,
                widest_plan: self.widest_plan,
                // Measured rather than assumed to be zero. A green run
                // should report the number it proved, not a constant that
                // happens to be right.
                widest_gap: self.gaps.values().map(|gap| gap.worst).max().unwrap_or(0),
            }),
        };

        let groups = self
            .groups
            .iter()
            .map(|(group, state)| GroupView {
                group: *group,
                incarnation: state.incarnation,
                state: state.state,
                poisoned: state.poisoned,
                stalled: state.stalled,
                counter: state.counter,
                queued: queue_len(state),
                quota: state.quota,
                servicing: self.occupied(*group),
            })
            .collect::<Vec<_>>();
        let live_groups = groups
            .iter()
            .filter(|view| {
                !matches!(
                    view.state,
                    GroupLifecycle::Removed | GroupLifecycle::Tombstoned
                )
            })
            .count();
        let poisoned_groups = groups.iter().filter(|view| view.poisoned).count();
        let ready_groups = self
            .groups
            .keys()
            .filter(|group| self.is_ready(**group))
            .count();

        Replay {
            summary: SchedulerSummary {
                live_groups: u32::try_from(live_groups).unwrap_or(u32::MAX),
                poisoned_groups: u32::try_from(poisoned_groups).unwrap_or(u32::MAX),
                ready_groups: u32::try_from(ready_groups).unwrap_or(u32::MAX),
                queued: self.queued,
                admitted: self.admitted,
                serviced: self.serviced,
                failed: self.failed,
            },
            view: SchedulerView {
                groups,
                queued: self.queued,
            },
            outcomes: self.outcomes,
            services: self.services,
            failures: self.failures,
            fairness,
            fault: fault.map(|(site, _)| site),
        }
    }

    /// Returns the worst opportunity gap the history produced, when any group
    /// was ready as a plan was armed and left out of it. Ties go to the lowest
    /// group ID so that one history always names one group.
    fn widest_gap(&self) -> Option<SchedulingViolation> {
        self.gaps
            .iter()
            .filter(|(_, gap)| gap.worst > 0)
            .max_by_key(|(group, gap)| (gap.worst, std::cmp::Reverse(**group)))
            .map(|(group, gap)| SchedulingViolation::OpportunityGap {
                group: *group,
                from_pass: gap.worst_from.unwrap_or_else(PassIndex::first),
                denied_passes: gap.worst,
            })
    }
}

fn queue_len(state: &DerivedGroup) -> u32 {
    u32::try_from(state.queue.len()).unwrap_or(u32::MAX)
}

/// Recomputes what a client session owes one submission.
///
/// `Some` is the answer the gate must give; `None` means the request is new
/// work. The checks run in a fixed order — epoch, then envelope, then sequence
/// — because a request whose fingerprint does not describe its own command is
/// malformed wherever its sequence falls.
fn session_verdict(
    session: &DerivedSession,
    request: RequestIdentity,
    command: CounterCommand,
) -> Option<AdmissionOutcome> {
    let rejected = |rejection| Some(AdmissionOutcome::Rejected(rejection));
    if request.session_epoch < session.epoch {
        return rejected(AdmissionRejection::StaleSession {
            current: session.epoch,
        });
    }
    if request.session_epoch > session.epoch {
        return rejected(AdmissionRejection::FutureSession {
            current: session.epoch,
        });
    }
    let recomputed = RequestFingerprint::of(&command);
    if request.fingerprint != recomputed {
        return rejected(AdmissionRejection::FingerprintMismatch {
            expected: recomputed,
        });
    }

    let expected = match session.completed {
        None => Sequence::first(),
        Some((completed, cached, result)) => match request.sequence.cmp(&completed) {
            Ordering::Less => {
                return rejected(AdmissionRejection::StaleSequence { highest: completed })
            }
            Ordering::Equal if command == cached => {
                return Some(AdmissionOutcome::Replayed { result })
            }
            Ordering::Equal => return rejected(AdmissionRejection::ConflictingRetry),
            // The `Greater` arm names a sequence above the completed one, so
            // the completed one cannot be the numeric maximum and its successor
            // exists. An exhaustion refusal here would describe a request no
            // client can construct.
            Ordering::Greater => completed
                .successor()
                .expect("nothing outranks the maximum, so a greater sequence proves a successor"),
        },
    };

    match session.outstanding {
        Some((outstanding, queued, id)) if request.sequence == outstanding => {
            if command == queued {
                Some(AdmissionOutcome::AlreadyQueued { work: id })
            } else {
                rejected(AdmissionRejection::ConflictingRetry)
            }
        }
        _ if request.sequence == expected => None,
        _ => rejected(AdmissionRejection::SequenceGap { expected }),
    }
}

#[cfg(test)]
mod tests {
    // Aliased on purpose: this module scans the file it lives in, so a literal
    // `FaultSite::Something` written here would be one of the occurrences it is
    // counting.
    use super::FaultSite as Site;

    /// This file's own source, so the check below is over the code it is about.
    const SOURCE: &str = include_str!("oracle.rs");

    /// Every site names exactly one check, and every check names a site.
    ///
    /// The exhaustive match in `tests/redteam_controls.rs` closes the other
    /// direction — a site with no control does not compile — and the const
    /// check above closes a third: a site that `ALL` does not reach. Neither
    /// closes this one. Two checks passing the same site would leave one of
    /// them with no deliberate violator while every compile-time mechanism
    /// stayed satisfied, and detecting it from the type system needs variant
    /// reflection that a crate with no dependencies has no way to obtain. So it
    /// is decided from the text instead, which is the honest form of "this part
    /// is not the compiler's".
    #[test]
    fn every_fault_site_is_raised_by_exactly_one_check() {
        let calls = concat!("self.", "fault(");
        assert_eq!(
            SOURCE.matches(calls).count(),
            Site::COUNT - 1,
            "every site but the fairness bound is one fault call, and every \
             fault call is a site"
        );
        for site in Site::ALL {
            let needle = format!("FaultSite::{site:?}");
            assert_eq!(
                SOURCE.matches(needle.as_str()).count(),
                1,
                "{site:?} must be named by exactly one check"
            );
        }
        let marker = format!("FaultSite::{:?}", Site::EndOfSites);
        assert_eq!(
            SOURCE.matches(marker.as_str()).count(),
            0,
            "the end marker is not a site, and no check may raise it"
        );
    }
}
