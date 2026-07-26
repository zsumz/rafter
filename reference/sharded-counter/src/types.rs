use std::num::{NonZeroI64, NonZeroU32, NonZeroU64};

/// Index of one shard group within the configured group range.
///
/// A group ID names an administrative slot, not an incarnation of it. A slot
/// that has been removed and created again is the same ID and a different
/// [`GroupIncarnation`]; work must carry both.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct GroupId(u32);

impl GroupId {
    /// Creates a group identifier.
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// Returns the numeric slot.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// Nonzero generation of one group slot.
///
/// Creating a slot that was previously removed produces a strictly greater
/// incarnation. This is what makes "a removed group is not resurrected by late
/// traffic" decidable: the traffic names an incarnation, and a stale one is
/// rejected whether or not the slot is live again.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct GroupIncarnation(NonZeroU32);

impl GroupIncarnation {
    /// Creates a group incarnation, rejecting zero.
    #[must_use]
    pub const fn new(value: u32) -> Option<Self> {
        match NonZeroU32::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the incarnation issued by a slot's first creation.
    #[must_use]
    pub const fn first() -> Self {
        match Self::new(1) {
            Some(incarnation) => incarnation,
            None => unreachable!(),
        }
    }

    /// Returns the numeric generation.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }

    /// Returns the next incarnation of the same slot, or `None` when the slot's
    /// generation space is exhausted.
    ///
    /// Exhaustion fails closed. Wrapping would let a late message name a slot
    /// generation that has already been retired.
    #[must_use]
    pub const fn successor(self) -> Option<Self> {
        match self.0.get().checked_add(1) {
            Some(value) => Self::new(value),
            None => None,
        }
    }
}

/// Index of one bounded per-group client-session slot.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ClientId(u32);

impl ClientId {
    /// Creates a client slot identifier.
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// Returns the numeric slot.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// Nonzero session generation for one client slot within one group.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct SessionEpoch(NonZeroU64);

impl SessionEpoch {
    /// Creates a session epoch, rejecting zero.
    #[must_use]
    pub const fn new(value: u64) -> Option<Self> {
        match NonZeroU64::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the numeric epoch.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Nonzero monotone request sequence within one session.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Sequence(NonZeroU64);

impl Sequence {
    /// Creates a request sequence, rejecting zero.
    #[must_use]
    pub const fn new(value: u64) -> Option<Self> {
        match NonZeroU64::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the only sequence a fresh session may submit.
    #[must_use]
    pub const fn first() -> Self {
        match Self::new(1) {
            Some(sequence) => sequence,
            None => unreachable!(),
        }
    }

    /// Returns the numeric sequence.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }

    /// Returns the successor sequence, or `None` at the numeric maximum.
    #[must_use]
    pub const fn successor(self) -> Option<Self> {
        match self.0.get().checked_add(1) {
            Some(value) => Self::new(value),
            None => None,
        }
    }
}

/// Stable identifier for one admitted unit of work.
///
/// Work IDs are issued by the scheduler at admission and are unique for the
/// life of one scheduler. They exist so an observer can pair an admission with
/// the service or failure that retired it, which is what makes the
/// work-conservation law checkable.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct WorkId(NonZeroU64);

impl WorkId {
    /// Creates a work identifier, rejecting zero.
    #[must_use]
    pub const fn new(value: u64) -> Option<Self> {
        match NonZeroU64::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the numeric identifier.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Monotone tick counter driving the scheduler.
///
/// A tick is the scheduler's unit of attention, not a duration. Nothing in this
/// contract expires after a real-world interval.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct TickIndex(u64);

impl TickIndex {
    /// Tick index before the scheduler has stepped.
    pub const ZERO: Self = Self(0);

    /// Creates a tick index.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the numeric tick.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Nonzero index of one complete pass over the ready set.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct PassIndex(NonZeroU64);

impl PassIndex {
    /// Creates a pass index, rejecting zero.
    #[must_use]
    pub const fn new(value: u64) -> Option<Self> {
        match NonZeroU64::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the index of a scheduler's first pass.
    #[must_use]
    pub const fn first() -> Self {
        match Self::new(1) {
            Some(pass) => pass,
            None => unreachable!(),
        }
    }

    /// Returns the numeric index.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }

    /// Returns the next pass index, or `None` when the pass space is exhausted.
    #[must_use]
    pub const fn successor(self) -> Option<Self> {
        match self.0.get().checked_add(1) {
            Some(value) => Self::new(value),
            None => None,
        }
    }
}

/// Nonzero worker occupancy of one unit of work, measured in ticks.
///
/// Cost is the resource a unit of work consumes, not a latency promise. A group
/// with deliberately slow storage is modeled as a group whose work costs more
/// ticks, and nothing else about it changes.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ServiceCost(NonZeroU32);

impl ServiceCost {
    /// Creates a service cost, rejecting zero.
    ///
    /// Zero is unrepresentable because free work would let one group hold a
    /// worker forever without the occupancy that makes exhaustion observable.
    #[must_use]
    pub const fn new(value: u32) -> Option<Self> {
        match NonZeroU32::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the numeric cost in ticks.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

/// Nonzero bound on the work one group may consume in one scheduling
/// opportunity.
///
/// A quota decides a group's throughput share. It never decides its
/// *opportunity* share, which the pass alone governs. Raising one group's quota
/// therefore lets it do more per pass without letting it take another group's
/// turn.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct WorkQuota(NonZeroU32);

impl WorkQuota {
    /// Creates a work quota, rejecting zero.
    ///
    /// A zero quota would put a group in the ready set that no opportunity
    /// could ever drain, which is starvation wearing a configuration's clothes.
    #[must_use]
    pub const fn new(value: u32) -> Option<Self> {
        match NonZeroU32::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the numeric quota.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

/// Nonzero signed counter adjustment.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Delta(NonZeroI64);

impl Delta {
    /// Creates a counter delta, rejecting zero.
    ///
    /// A zero delta is rejected at construction rather than applied as a no-op,
    /// because a mutation that cannot change state still consumes a request
    /// sequence and a queue slot, and admitting one would make the queue bound
    /// depend on traffic that means nothing.
    #[must_use]
    pub const fn new(value: i64) -> Option<Self> {
        match NonZeroI64::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the numeric delta.
    #[must_use]
    pub const fn get(self) -> i64 {
        self.0.get()
    }
}

/// Service priority class of one unit of work.
///
/// The ordering on this type is its declaration order, which is *descending*
/// service priority: `Control < Command < Snapshot < Bulk`, and a group
/// services the least class it has queued first.
///
/// This ordering decides which of a group's own items fill its quota. It never
/// reorders the pass, so a group with urgent control work does not take another
/// group's turn. Class priority and pass fairness are deliberately different
/// axes.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum WorkClass {
    /// Heartbeat and election traffic.
    ///
    /// First within every opportunity. Losing an election because a heartbeat
    /// queued behind a snapshot chunk is the failure this ordering exists to
    /// prevent.
    Control,
    /// Client counter commands.
    ///
    /// Client-visible progress outranks catching a lagging peer up.
    Command,
    /// Snapshot build and transfer pressure.
    ///
    /// Ahead of bulk replication because a snapshot exists to *replace* bulk
    /// catch-up; deferring it makes the bulk backlog it would have retired
    /// larger.
    Snapshot,
    /// Bulk log replication catch-up.
    ///
    /// Last by construction. Sustained higher-class load starves bulk work, and
    /// that is intended: this contract promises bulk progress only in the
    /// absence of saturating control, command, and snapshot traffic.
    Bulk,
}

/// Every work class in descending service priority.
pub const WORK_CLASS_ORDER: [WorkClass; 4] = [
    WorkClass::Control,
    WorkClass::Command,
    WorkClass::Snapshot,
    WorkClass::Bulk,
];

impl WorkClass {
    /// Returns the class's position in [`WORK_CLASS_ORDER`].
    #[must_use]
    pub const fn rank(self) -> usize {
        match self {
            Self::Control => 0,
            Self::Command => 1,
            Self::Snapshot => 2,
            Self::Bulk => 3,
        }
    }
}

/// Abstract non-command work class.
///
/// These are the scheduler's own traffic. This crate models what they cost and
/// where they sit in the priority order, and deliberately nothing else: a
/// snapshot-pressure hook here is a queued item with a class and a cost, not a
/// snapshot.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum SystemClass {
    /// Heartbeat and election traffic.
    Control,
    /// Snapshot build and transfer pressure.
    Snapshot,
    /// Bulk log replication catch-up.
    Bulk,
}

impl SystemClass {
    /// Returns the work class this system traffic is scheduled in.
    #[must_use]
    pub const fn class(self) -> WorkClass {
        match self {
            Self::Control => WorkClass::Control,
            Self::Snapshot => WorkClass::Snapshot,
            Self::Bulk => WorkClass::Bulk,
        }
    }
}

/// Deterministic per-group counter command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CounterCommand {
    /// Adjusts the group's counter by a nonzero delta.
    Add {
        /// Nonzero adjustment.
        delta: Delta,
    },
    /// Reads the group's counter.
    ///
    /// Reads are scheduled work like any mutation. They occupy a queue slot,
    /// consume quota, and carry a request identity, because a read that skipped
    /// the queue would be a second admission path with its own bounds.
    Read,
}

/// One unit of schedulable work.
///
/// The two shapes are distinct rather than one struct with an optional payload
/// so that a control item can never carry a counter command and a counter
/// command can never be scheduled outside its own class.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Work {
    /// A client counter request, scheduled in [`WorkClass::Command`].
    Counter {
        /// Bounded request identity.
        request: RequestIdentity,
        /// Command to apply when the item is serviced.
        command: CounterCommand,
        /// Worker occupancy this item costs.
        cost: ServiceCost,
    },
    /// Abstract scheduler traffic in a non-command class.
    System {
        /// Class this traffic is scheduled in.
        class: SystemClass,
        /// Worker occupancy this item costs.
        cost: ServiceCost,
    },
    /// Work whose application fails irrecoverably and poisons its group.
    ///
    /// This is the poison injection point, and it is a work shape rather than a
    /// scheduler command on purpose: a group is poisoned by what its own work
    /// did, never by an operator asking for it, and the isolation property is
    /// only interesting when the poison arrives through the ordinary queue.
    Faulty {
        /// Class this item was scheduled in before it failed.
        class: SystemClass,
        /// Worker occupancy consumed before the failure surfaced.
        cost: ServiceCost,
    },
}

impl Work {
    /// Returns the class this work is scheduled in.
    #[must_use]
    pub const fn class(self) -> WorkClass {
        match self {
            Self::Counter { .. } => WorkClass::Command,
            Self::System { class, .. } | Self::Faulty { class, .. } => class.class(),
        }
    }

    /// Returns the worker occupancy this work costs.
    #[must_use]
    pub const fn cost(self) -> ServiceCost {
        match self {
            Self::Counter { cost, .. } | Self::System { cost, .. } | Self::Faulty { cost, .. } => {
                cost
            }
        }
    }

    /// Returns the request identity a counter item carries.
    #[must_use]
    pub const fn request_identity(self) -> Option<RequestIdentity> {
        match self {
            Self::Counter { request, .. } => Some(request),
            Self::System { .. } | Self::Faulty { .. } => None,
        }
    }
}

/// Deterministic digest of the counter command an identity claims to carry.
///
/// The digest binds a request identity to the command the client believes it
/// sent, which is what lets a retry after an unknown outcome be routed. It is
/// never the admission key: retry and conflict decisions compare the exact
/// bounded command, so a collision can never admit a conflicting retry as an
/// exact one.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RequestFingerprint(u64);

impl RequestFingerprint {
    /// Computes the fingerprint of a counter command's canonical encoding.
    #[must_use]
    pub fn of(command: &CounterCommand) -> Self {
        let mut digest = Digest::new();
        match command {
            CounterCommand::Add { delta } => {
                digest.tag(1);
                digest.word(delta.get().to_le_bytes());
            }
            CounterCommand::Read => digest.tag(2),
        }
        Self(digest.finish())
    }

    /// Rebuilds a fingerprint from a digest that arrived from a client.
    ///
    /// An envelope must round-trip the digest a client actually sent, including
    /// one that does not describe its own command. Recomputing it here would
    /// silently repair the malformed request that
    /// [`AdmissionRejection::FingerprintMismatch`] exists to reject.
    #[must_use]
    pub const fn from_digest(digest: u64) -> Self {
        Self(digest)
    }

    /// Returns the numeric digest.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

struct Digest(u64);

impl Digest {
    fn new() -> Self {
        Self(FNV_OFFSET_BASIS)
    }

    fn tag(&mut self, value: u8) {
        self.byte(value);
    }

    fn byte(&mut self, value: u8) {
        self.0 ^= u64::from(value);
        self.0 = self.0.wrapping_mul(FNV_PRIME);
    }

    /// Absorbs an already little-endian eight-byte field. The caller supplies
    /// the bytes so that a signed delta needs no sign cast to be digested.
    fn word(&mut self, bytes: [u8; 8]) {
        for byte in bytes {
            self.byte(byte);
        }
    }

    fn finish(self) -> u64 {
        self.0
    }
}

/// Identity of one counter request within one group's client session.
///
/// Sessions are scoped to a group. A client addressing two groups holds two
/// independent sessions, which is what lets one client have one mutation
/// outstanding *per group* rather than one across the whole service.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestIdentity {
    /// Bounded client slot within the group.
    pub client_id: ClientId,
    /// Exact active session generation for that slot.
    pub session_epoch: SessionEpoch,
    /// Monotone sequence within the session.
    pub sequence: Sequence,
    /// Digest the client claims describes its command.
    pub fingerprint: RequestFingerprint,
}

/// Administrative lifecycle state of one group slot.
///
/// This is the operator's axis. Poison is a separate health axis: a poisoned
/// group keeps its lifecycle state and stops being serviceable, so the ordinary
/// drain and removal path is still the way it leaves.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupLifecycle {
    /// The slot exists and its durable state is being established.
    ///
    /// Not serviceable. Nothing may be admitted yet.
    Creating,
    /// The slot is replaying durable state and catching up.
    ///
    /// Serviceable for system traffic only. Client commands are refused rather
    /// than parked: queueing them behind a recovery of unknown length converts
    /// one slow group into a queue-limit outage for the whole host.
    Recovering,
    /// The slot is fully serviceable.
    Serving,
    /// The slot accepts no new work and is retiring what it already accepted.
    ///
    /// Serviceable, because draining is how accepted work leaves without being
    /// discarded.
    Draining,
    /// The slot is gone and its counter, sessions, and queue are gone with it.
    ///
    /// The ID may be created again as a strictly greater incarnation.
    Removed,
    /// The slot is gone permanently and its ID may never be created again.
    Tombstoned,
}

impl GroupLifecycle {
    /// Returns whether the state admits any scheduling at all.
    #[must_use]
    pub const fn is_serviceable(self) -> bool {
        matches!(self, Self::Recovering | Self::Serving | Self::Draining)
    }

    /// Returns whether the state admits newly submitted work of a class.
    ///
    /// A draining group admits nothing: that is the whole content of draining.
    #[must_use]
    pub const fn admits(self, class: WorkClass) -> bool {
        match self {
            Self::Serving => true,
            Self::Recovering => !matches!(class, WorkClass::Command),
            Self::Creating | Self::Draining | Self::Removed | Self::Tombstoned => false,
        }
    }
}

/// Administrative transition requested for one group slot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LifecycleRequest {
    /// Brings a slot into existence, or reopens a removed one.
    Create {
        /// Work quota for this incarnation.
        quota: WorkQuota,
    },
    /// Moves a created slot into recovery.
    Recover,
    /// Declares a recovered slot ready to serve clients.
    Serve,
    /// Stops admission and begins retiring accepted work.
    Drain,
    /// Removes a drained slot.
    Remove,
    /// Marks a removed slot permanently unusable.
    Tombstone,
}

impl LifecycleRequest {
    /// Returns the state this request asks the slot to reach.
    #[must_use]
    pub const fn target(self) -> GroupLifecycle {
        match self {
            Self::Create { .. } => GroupLifecycle::Creating,
            Self::Recover => GroupLifecycle::Recovering,
            Self::Serve => GroupLifecycle::Serving,
            Self::Drain => GroupLifecycle::Draining,
            Self::Remove => GroupLifecycle::Removed,
            Self::Tombstone => GroupLifecycle::Tombstoned,
        }
    }
}

/// Stable result of one lifecycle request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LifecycleOutcome {
    /// The slot came into existence under the reported incarnation.
    ///
    /// A first creation and a reopening are one outcome because they differ in
    /// exactly one observable way, and that way is the incarnation: first
    /// creations report [`GroupIncarnation::first`] and reopenings report a
    /// strictly greater one.
    Created {
        /// Incarnation the slot now carries.
        incarnation: GroupIncarnation,
    },
    /// The slot moved between two existing states.
    Applied {
        /// State the slot left.
        from: GroupLifecycle,
        /// State the slot reached.
        to: GroupLifecycle,
        /// Incarnation the slot carries after the transition.
        incarnation: GroupIncarnation,
    },
    /// The slot was already in the requested state and did not move.
    Idempotent {
        /// Unchanged state.
        state: GroupLifecycle,
        /// Unchanged incarnation.
        incarnation: GroupIncarnation,
    },
    /// The request was refused.
    Rejected(LifecycleRejection),
}

/// Explicit refusal of a lifecycle request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LifecycleRejection {
    /// The group ID is outside the configured range.
    GroupOutOfRange,
    /// The requested transition is not a successor of the current state.
    Conflict {
        /// State the slot is in.
        current: GroupLifecycle,
        /// State the request asked for.
        requested: GroupLifecycle,
    },
    /// The slot has never been created.
    GroupUnknown,
    /// The slot is tombstoned and accepts nothing further.
    GroupTombstoned,
    /// A creation repeated for a live slot named a different quota.
    ///
    /// Reported rather than absorbed as idempotent: a quota belongs to an
    /// incarnation, so accepting the repeat would discard the number the caller
    /// asked for while telling it nothing changed.
    QuotaConflict {
        /// Quota the incarnation was created with.
        current: WorkQuota,
    },
    /// Removal was requested while accepted work was still queued.
    ///
    /// This is the enforcement point for "accepted work is never discarded":
    /// the only way past it is draining, which either services the work or
    /// reports every item it could not.
    QueueNotDrained {
        /// Items still queued for the slot.
        pending: u32,
    },
    /// The slot's incarnation space is exhausted and it can never reopen.
    IncarnationExhausted,
}

/// One lifecycle request's outcome together with anything it retired.
///
/// The two travel together because draining a poisoned group is the one
/// transition that can retire accepted work, and a caller that learned only
/// that the transition applied would have no record of what it cost.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LifecycleTransition {
    /// What the request did to the slot.
    pub outcome: LifecycleOutcome,
    /// Accepted items the transition retired without service, in queue order.
    pub failed: Vec<FailureRecord>,
}

impl LifecycleTransition {
    /// Builds a transition that refused the request and retired nothing.
    #[must_use]
    pub const fn rejected(rejection: LifecycleRejection) -> Self {
        Self {
            outcome: LifecycleOutcome::Rejected(rejection),
            failed: Vec::new(),
        }
    }
}

/// Deterministic result of an applied counter command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CounterResult {
    /// The counter moved to the reported value.
    Added {
        /// Value after the delta was applied.
        value: i64,
    },
    /// The counter was read.
    Value {
        /// Value observed at service time.
        value: i64,
    },
    /// The command was admitted under its request identity and refused by
    /// counter rules.
    Rejected(CounterRejection),
}

/// Counter-level refusal that consumes and caches its request sequence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CounterRejection {
    /// The delta would push the counter past a numeric bound.
    ///
    /// Overflow fails closed rather than saturating: a saturated counter that
    /// silently stopped counting would satisfy every aggregate check in this
    /// crate while losing the adds that reached it.
    CounterOverflow {
        /// Value the counter kept.
        current: i64,
    },
}

/// Stable result of submitting one unit of work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionOutcome {
    /// The work took a queue slot and will be serviced.
    Queued {
        /// Identifier that will name the item's service or failure.
        work: WorkId,
    },
    /// An exact retry of a request that is already queued and not yet serviced.
    ///
    /// No second slot is taken. The original item carries the effect, and its
    /// result answers both attempts.
    AlreadyQueued {
        /// Identifier of the item already holding the slot.
        work: WorkId,
    },
    /// An exact retry of the session's highest completed request.
    ///
    /// Answered from the session cache without taking a queue slot, so an
    /// acknowledged request can still be confirmed while the queue is full.
    Replayed {
        /// Result the original execution produced.
        result: CounterResult,
    },
    /// The work was refused before it took a slot.
    Rejected(AdmissionRejection),
}

/// Explicit refusal at the admission gate. None of these consumes a sequence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionRejection {
    /// The group ID is outside the configured range.
    GroupOutOfRange,
    /// The slot has never been created.
    GroupUnknown,
    /// The slot is tombstoned and accepts nothing, forever.
    GroupTombstoned,
    /// The work named an incarnation that is no longer current.
    ///
    /// Reported whether or not the slot is live again under a later
    /// incarnation. Late traffic never resurrects a removed group and never
    /// lands on its replacement.
    StaleIncarnation {
        /// Incarnation the slot carries.
        current: GroupIncarnation,
    },
    /// The work named an incarnation the slot has not reached.
    FutureIncarnation {
        /// Incarnation the slot carries.
        current: GroupIncarnation,
    },
    /// The slot's lifecycle state does not admit work of this class.
    GroupNotAcceptingWork {
        /// State the slot is in.
        state: GroupLifecycle,
        /// Class that was refused.
        class: WorkClass,
    },
    /// The slot is poisoned and can service nothing until it is drained.
    GroupPoisoned,
    /// The slot's queue is full.
    GroupQueueFull {
        /// Configured per-group queue bound.
        limit: u32,
    },
    /// The scheduler's global queue is full.
    ///
    /// Checked after the per-group bound so that a group over its own limit
    /// learns which bound it hit rather than being told the host is busy.
    GlobalQueueFull {
        /// Configured global queue bound.
        limit: u32,
    },
    /// The client ID is outside the group's configured slot range.
    ClientOutOfRange,
    /// No session is open for the client slot in this group.
    SessionNotOpen,
    /// The request names an older session generation.
    StaleSession {
        /// Generation the slot carries.
        current: SessionEpoch,
    },
    /// The request names a newer generation that must be opened first.
    FutureSession {
        /// Generation the slot carries.
        current: SessionEpoch,
    },
    /// The sequence is older than the session's highest completed request.
    StaleSequence {
        /// Highest completed sequence.
        highest: Sequence,
    },
    /// The sequence skipped the required next value.
    ///
    /// A client with an outstanding request that submits the sequence after it
    /// arrives here: the outstanding request has not completed, so the expected
    /// next sequence has not moved.
    SequenceGap {
        /// Sequence the session will accept next.
        expected: Sequence,
    },
    /// A completed or outstanding identity was reused with another command.
    ConflictingRetry,
    /// The session's sequence space is exhausted.
    ///
    /// The client must open a greater session epoch before submitting again.
    /// Wrapping would let a fresh request land on a cached completion.
    SequenceExhausted,
    /// The supplied fingerprint does not describe the supplied command.
    FingerprintMismatch {
        /// Digest of the command that was actually supplied.
        expected: RequestFingerprint,
    },
    // There is deliberately no session-table capacity refusal. The addressable
    // client range is the table's bound, so [`Self::ClientOutOfRange`] refuses
    // every request that could have overflowed it, and a second refusal for a
    // state that cannot be reached would be a promise about behavior no test
    // could ever observe.
}

/// Stable result of opening a session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionOutcome {
    /// A previously unused client slot opened.
    Opened {
        /// Generation now active.
        session_epoch: SessionEpoch,
    },
    /// A greater generation replaced the old one and cleared its cache.
    Replaced {
        /// Generation now active.
        session_epoch: SessionEpoch,
    },
    /// The requested generation was already open.
    AlreadyOpen {
        /// Generation already active.
        session_epoch: SessionEpoch,
    },
    /// The request was refused.
    ///
    /// Opening a session is an admission-gate action, so it reports the same
    /// refusals as work submission rather than a second vocabulary that could
    /// disagree with it.
    Rejected(AdmissionRejection),
}

/// Why a group in a pass plan was offered a turn it could not take.
///
/// A skip is still an opportunity: the fairness bound is about being offered a
/// turn, and a group that cannot use the turn it was offered has not been
/// starved of anything.
///
/// There is exactly one reason, and that is a property rather than an
/// oversight. Two of the ways a group could stop being ready — its queue
/// emptying and its own work poisoning it — each require the dispatch that this
/// same offer would have been, so neither can happen first. The third is
/// different: an operator may move a planned group's lifecycle at any time,
/// with no dispatch involved. It still cannot revoke readiness, because the
/// only edge that would is removal, and removal is refused while the group
/// holds a queue it has not drained — which is the same queue that made it
/// ready. That leaves an external readiness signal as the one revocation a pass
/// can actually observe.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SkipReason {
    /// An external readiness signal stalled the group after the plan was armed.
    Stalled,
}

/// External availability of one group, as reported to the scheduler.
///
/// This is the only readiness input the scheduler does not derive for itself.
/// It models backpressure the host learns about from outside — a storage device
/// that stopped accepting writes, a peer link that closed — and it is sticky: a
/// stalled group stays out of the ready set until it is reported available
/// again, however much work it accumulates.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupAvailability {
    /// The group may be dispatched.
    Available,
    /// The group may not be dispatched, whatever its queue holds.
    Stalled,
}

/// One external readiness report for one group.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadinessSignal {
    /// Group the report concerns.
    pub group: GroupId,
    /// Availability the host observed.
    pub availability: GroupAvailability,
}

impl ReadinessSignal {
    /// Reports a group as dispatchable again.
    #[must_use]
    pub const fn available(group: GroupId) -> Self {
        Self {
            group,
            availability: GroupAvailability::Available,
        }
    }

    /// Reports a group as blocked by external backpressure.
    #[must_use]
    pub const fn stalled(group: GroupId) -> Self {
        Self {
            group,
            availability: GroupAvailability::Stalled,
        }
    }
}

/// What happened when a group in the pass plan was offered its turn.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OfferOutcome {
    /// A worker took the group and serviced work under its quota.
    Dispatched {
        /// Items serviced in this opportunity.
        serviced: u32,
        /// Worker occupancy the opportunity consumed, in ticks.
        ///
        /// This is the sum of the [`ServiceCost`]s of the items the turn
        /// serviced, and it is *derivable*: an observer that knows the group's
        /// queue knows what the turn had to cost. The reference oracle derives
        /// it and rejects a dispatch that claims anything else, so this number
        /// is a report rather than a self-certification.
        ///
        /// The width is the reason it cannot silently under-charge. A turn
        /// services at most [`WorkQuota`] items, each costing at most
        /// [`ServiceCost`]; both are `u32`, so the largest turn any
        /// configuration admits costs `(2^32 - 1)^2`, which is below `u64::MAX`.
        /// A `u32` accumulator saturated instead, and a saturated occupancy is
        /// an under-charge that nothing downstream could detect.
        cost: u64,
    },
    /// The group was offered its turn and had nothing to take.
    Skipped(SkipReason),
}

impl OfferOutcome {
    /// Returns the number of items the opportunity serviced.
    #[must_use]
    pub const fn serviced(self) -> u32 {
        match self {
            Self::Dispatched { serviced, .. } => serviced,
            Self::Skipped(_) => 0,
        }
    }

    /// Returns the worker occupancy the opportunity consumed, in ticks.
    #[must_use]
    pub const fn cost(self) -> u64 {
        match self {
            Self::Dispatched { cost, .. } => cost,
            Self::Skipped(_) => 0,
        }
    }
}

/// One group's turn within one pass.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Offer {
    /// Group whose turn it was.
    pub group: GroupId,
    /// What the group did with it.
    pub outcome: OfferOutcome,
}

/// Why a pass did not finish within the tick that advanced it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PassSuspension {
    /// Every worker is occupied.
    ///
    /// This is the "global resource exhaustion" the fairness bound excepts. The
    /// pass keeps its remaining plan and resumes at the same cursor, so the
    /// exception costs the unoffered groups time and never their turn.
    NoFreeWorker,
}

/// What a tick did to the pass in progress.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PassProgress {
    /// No group was ready, so no plan was armed.
    Idle,
    /// The pass advanced and remains open.
    Suspended(PassSuspension),
    /// Every group in the plan was offered its turn and the plan retired.
    Completed,
}

/// One serviced unit of work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServiceRecord {
    /// Item that was serviced.
    pub work: WorkId,
    /// Group that serviced it.
    pub group: GroupId,
    /// Class it was serviced in.
    pub class: WorkClass,
    /// Counter result, for command-class work.
    pub result: Option<CounterResult>,
}

/// One accepted item retired without being serviced.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FailureRecord {
    /// Item that was retired.
    pub work: WorkId,
    /// Group that held it.
    pub group: GroupId,
    /// Why it could not be serviced.
    pub reason: WorkFailure,
}

/// Why accepted work was retired without service.
///
/// Every accepted item reaches exactly one terminal disposition: serviced, or
/// failed for one of these reasons. There is no third outcome and no silent
/// one.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkFailure {
    /// The group was poisoned, and draining reported the loss item by item.
    GroupPoisoned,
}

/// Everything one tick did.
///
/// The scheduler retains none of this. A report is emitted once and forgotten,
/// which is what keeps the scheduler's own state bounded no matter how long it
/// runs; an observer that wants the history is the one that pays for it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TickReport {
    /// Tick this report describes.
    pub tick: TickIndex,
    /// Pass the tick advanced, when one was open or armed.
    pub pass: Option<PassIndex>,
    /// Plan armed during this tick, in offer order.
    pub armed: Option<Vec<GroupId>>,
    /// Turns taken during this tick, in order.
    pub offers: Vec<Offer>,
    /// Work serviced during this tick, in order.
    pub serviced: Vec<ServiceRecord>,
    /// Workers whose occupancy ended at this tick, in group order.
    pub released: Vec<GroupId>,
    /// What the tick did to the pass.
    pub progress: PassProgress,
}

/// Public deterministic inspection of one live group.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupView {
    /// Slot this view describes.
    pub group: GroupId,
    /// Current incarnation.
    pub incarnation: GroupIncarnation,
    /// Current lifecycle state.
    pub state: GroupLifecycle,
    /// Whether the group's own work has poisoned it.
    pub poisoned: bool,
    /// Whether an external readiness signal is holding the group back.
    pub stalled: bool,
    /// Counter value.
    pub counter: i64,
    /// Items queued and not yet retired.
    pub queued: u32,
    /// Work quota for this incarnation.
    pub quota: WorkQuota,
    /// Whether the group is occupying a worker whose cost is not yet paid.
    ///
    /// This is a derived fact, not a scheduler-authored flag. An occupancy
    /// opens when a turn dispatches and ends exactly `cost` ticks later, where
    /// `cost` is the sum of the serviced items' [`ServiceCost`]s. A scheduler
    /// that simply stopped reporting the end of an occupancy would hold its
    /// group out of every future plan, which is why the end is derived rather
    /// than believed.
    pub servicing: bool,
}

/// Canonical deterministic state view shared only for differential assertions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchedulerView {
    /// Every slot that has ever been created, sorted by ID. Removed and
    /// tombstoned slots are reported alongside live ones, because a slot that
    /// has left still constrains what may address it.
    pub groups: Vec<GroupView>,
    /// Items queued across every group.
    pub queued: u32,
}

/// Aggregate counts for one scheduler.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SchedulerSummary {
    /// Slots that have been created and not yet removed.
    pub live_groups: u32,
    /// Live groups whose own work has poisoned them.
    pub poisoned_groups: u32,
    /// Groups currently in the ready set.
    pub ready_groups: u32,
    /// Items queued across every group.
    pub queued: u32,
    /// Items admitted over the scheduler's life.
    pub admitted: u64,
    /// Items serviced over the scheduler's life.
    pub serviced: u64,
    /// Items retired without service over the scheduler's life.
    pub failed: u64,
}

/// Fixed resource limits for one scheduler.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SchedulerConfig {
    max_groups: u32,
    workers: u32,
    max_clients_per_group: u32,
    max_group_queue: u32,
    max_global_queue: u32,
}

impl SchedulerConfig {
    /// Creates a bounded scheduler configuration.
    ///
    /// # Errors
    ///
    /// Returns an error when any bound is zero, or when the global queue bound
    /// is smaller than one group's, which would make the per-group bound
    /// unreachable and hide which limit a workload is actually hitting.
    pub const fn new(
        max_groups: u32,
        workers: u32,
        max_clients_per_group: u32,
        max_group_queue: u32,
        max_global_queue: u32,
    ) -> Result<Self, SchedulerConfigError> {
        if max_groups == 0 {
            return Err(SchedulerConfigError::ZeroGroups);
        }
        if workers == 0 {
            return Err(SchedulerConfigError::ZeroWorkers);
        }
        if max_clients_per_group == 0 {
            return Err(SchedulerConfigError::ZeroClients);
        }
        if max_group_queue == 0 {
            return Err(SchedulerConfigError::ZeroGroupQueue);
        }
        if max_global_queue < max_group_queue {
            return Err(SchedulerConfigError::GlobalQueueBelowGroupQueue);
        }
        Ok(Self {
            max_groups,
            workers,
            max_clients_per_group,
            max_group_queue,
            max_global_queue,
        })
    }

    /// Maximum number of addressable group slots.
    #[must_use]
    pub const fn max_groups(self) -> u32 {
        self.max_groups
    }

    /// Number of workers that may hold dispatches at once.
    ///
    /// Worker count changes how long a pass takes. It never changes which
    /// groups a pass contains, so the fairness bound does not mention it.
    #[must_use]
    pub const fn workers(self) -> u32 {
        self.workers
    }

    /// Maximum number of client session slots per group.
    #[must_use]
    pub const fn max_clients_per_group(self) -> u32 {
        self.max_clients_per_group
    }

    /// Maximum number of items one group may hold.
    #[must_use]
    pub const fn max_group_queue(self) -> u32 {
        self.max_group_queue
    }

    /// Maximum number of items the scheduler may hold across every group.
    #[must_use]
    pub const fn max_global_queue(self) -> u32 {
        self.max_global_queue
    }

    /// Returns whether a group ID addresses a configured slot.
    #[must_use]
    pub const fn admits_group(self, group: GroupId) -> bool {
        group.get() < self.max_groups
    }

    /// Returns whether a client ID addresses a configured session slot.
    #[must_use]
    pub const fn admits_client(self, client: ClientId) -> bool {
        client.get() < self.max_clients_per_group
    }
}

/// Invalid scheduler configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchedulerConfigError {
    /// No group could ever be created.
    ZeroGroups,
    /// No work could ever be dispatched.
    ZeroWorkers,
    /// No session could ever open.
    ZeroClients,
    /// No work could ever be queued.
    ZeroGroupQueue,
    /// The global bound is below one group's, making the group bound
    /// unreachable.
    GlobalQueueBelowGroupQueue,
}
