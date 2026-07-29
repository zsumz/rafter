use crate::{
    AdmissionOutcome, ClientId, CounterResult, GroupAvailability, GroupId, GroupIncarnation,
    LifecycleOutcome, LifecycleRequest, OfferOutcome, PassIndex, RequestIdentity, SessionEpoch,
    SessionOutcome, TickIndex, Work, WorkFailure, WorkId,
};

/// Stable identifier for one client- or operator-visible operation.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct OperationId(u64);

impl OperationId {
    /// Creates an operation identifier.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the numeric identifier.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// One operation a client or operator asked for.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Operation {
    /// Opening or advancing a client session inside a group.
    OpenSession {
        /// Group the session belongs to.
        group: GroupId,
        /// Incarnation the caller addressed.
        incarnation: GroupIncarnation,
        /// Client slot.
        client_id: ClientId,
        /// Generation the caller asked for.
        session_epoch: SessionEpoch,
    },
    /// Submitting one unit of work.
    Submit {
        /// Group the work is for.
        group: GroupId,
        /// Incarnation the caller addressed.
        incarnation: GroupIncarnation,
        /// Exact work submitted.
        work: Work,
    },
    /// Requesting an administrative lifecycle transition.
    Lifecycle {
        /// Group the request concerns.
        group: GroupId,
        /// Transition requested.
        request: LifecycleRequest,
    },
}

impl Operation {
    /// Returns the group the operation addressed.
    #[must_use]
    pub const fn group(self) -> GroupId {
        match self {
            Self::OpenSession { group, .. }
            | Self::Submit { group, .. }
            | Self::Lifecycle { group, .. } => group,
        }
    }

    /// Returns the request identity a counter submission carried.
    ///
    /// Retries share a request identity, so every attempt at one effect can be
    /// grouped from the history alone.
    #[must_use]
    pub const fn request_identity(self) -> Option<RequestIdentity> {
        match self {
            Self::Submit { work, .. } => work.request_identity(),
            Self::OpenSession { .. } | Self::Lifecycle { .. } => None,
        }
    }
}

/// What an operation was observed to produce.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationOutcome {
    /// A session request was answered.
    Session(SessionOutcome),
    /// A lifecycle request was answered.
    Lifecycle(LifecycleOutcome),
    /// A submission was answered at the admission gate, by a rejection, a
    /// replay from the session cache, or a queue slot.
    Admission(AdmissionOutcome),
    /// Queued work was serviced. Only command-class work carries a result.
    Serviced(Option<CounterResult>),
    /// Queued work was retired without service.
    Failed(WorkFailure),
}

/// A recorded event, retained for later checking.
///
/// A history is the ordered sequence of these events. Position in that sequence
/// *is* the real-time order.
///
/// The vocabulary has three families and one purpose. The first family records
/// what was asked for, the second what the scheduler decided, and the third
/// what a caller observed. That split is load bearing: an observer that folds
/// the first two families can recompute every answer for itself, and one that
/// folded the third would be copying the conclusions it is supposed to be
/// checking.
///
/// The vocabulary is closed and in-memory. It is deliberately not the real
/// adapter's replicated-command format: a history never enters the Raft log or
/// crosses a process boundary. Adding an outcome here is a contract change
/// recorded in `CONTRACT.md`, not a compatibility negotiation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HistoryEvent {
    /// A caller invoked an operation.
    Invoked {
        /// Operation identity unique within the history.
        operation_id: OperationId,
        /// Exact operation the caller issued.
        operation: Operation,
    },
    /// A caller observed a terminal outcome, deterministic refusals included.
    Completed {
        /// Operation identity from the matching invocation.
        operation_id: OperationId,
        /// Outcome the caller observed.
        outcome: OperationOutcome,
    },
    /// The caller could not tell what the operation did.
    ///
    /// The operation may or may not have taken a queue slot, so the caller must
    /// retry the *same* request identity and let the session's deduplication
    /// decide.
    Unknown {
        /// Operation identity from the matching invocation.
        operation_id: OperationId,
    },
    /// The submission provably never took a queue slot.
    ///
    /// Strictly stronger than [`HistoryEvent::Unknown`]: no copy of the attempt
    /// can be serviced later, so it changed no counter, consumed no sequence,
    /// and left its request identity free for a fresh attempt. A checker must
    /// treat it as never having happened.
    ///
    /// A rejection observed at the admission gate is an ordinary
    /// [`HistoryEvent::Completed`] event, not this one. This event exists for a
    /// lost outcome that a future transport can nonetheless prove was refused
    /// before it reached a queue; recording such a refusal as `Unknown` would
    /// let an implementation that serviced it be explained away.
    NotAdmitted {
        /// Operation identity from the matching invocation.
        operation_id: OperationId,
    },
    /// The host reported a group's external availability.
    AvailabilityReported {
        /// Tick the report was applied at.
        tick: TickIndex,
        /// Group the report concerns.
        group: GroupId,
        /// Availability the host observed.
        availability: GroupAvailability,
    },
    /// A worker's occupancy ended and its group may be dispatched again.
    ///
    /// This is a report of an instant, never a grant of one. The instant a
    /// dispatch's occupancy ends is its tick plus what the turn's services were
    /// worth — priced from the [`Self::WorkServiced`] stream, never from the
    /// cost the dispatch reported about itself — so an observer decides for
    /// itself whether this event arrived when it was due, early, or at all.
    WorkerReleased {
        /// Tick the worker came free at.
        tick: TickIndex,
        /// Group whose dispatch ended.
        group: GroupId,
    },
    /// A pass plan was armed from the ready set.
    ///
    /// The plan is the fairness bound's subject: every group named here is owed
    /// exactly one turn before this pass retires.
    PassArmed {
        /// Pass the plan belongs to.
        pass: PassIndex,
        /// Tick the plan was armed at.
        tick: TickIndex,
        /// Groups owed a turn, in offer order.
        plan: Vec<GroupId>,
    },
    /// A group in the armed plan took its turn.
    ///
    /// The tick is what makes a dispatch's worker occupancy derivable: an
    /// occupancy opens here and is due to end at `tick` plus what the turn's
    /// services were worth. Without the instant the turn was taken there is no
    /// deadline to hold the scheduler to, and an occupancy nobody can time out
    /// is a group nobody can prove was starved.
    GroupOffered {
        /// Pass the turn belonged to.
        pass: PassIndex,
        /// Tick the turn was taken at.
        tick: TickIndex,
        /// Group whose turn it was.
        group: GroupId,
        /// What the group did with it.
        outcome: OfferOutcome,
    },
    /// One queued item was serviced within a group's turn.
    ///
    /// A turn's services follow its [`Self::GroupOffered`] with nothing between
    /// them, because work is applied at dispatch and a turn is one indivisible
    /// act. That is what lets an observer price the turn: the services between
    /// one offer and the next recorded decision are exactly the work that turn
    /// did, so the first event that is not one of them ends the turn and fixes
    /// its cost. There is no tick here for the same reason — the turn's tick is
    /// the one the offer carries.
    WorkServiced {
        /// Pass the turn belonged to.
        pass: PassIndex,
        /// Group that serviced it.
        group: GroupId,
        /// Item that was serviced.
        work: WorkId,
    },
    /// Every group in the plan took its turn and the plan retired.
    PassCompleted {
        /// Pass that retired.
        pass: PassIndex,
        /// Tick it retired at.
        tick: TickIndex,
    },
}

impl HistoryEvent {
    /// Returns the operation identity an operation event belongs to.
    #[must_use]
    pub const fn operation_id(&self) -> Option<OperationId> {
        match *self {
            Self::Invoked { operation_id, .. }
            | Self::Completed { operation_id, .. }
            | Self::Unknown { operation_id }
            | Self::NotAdmitted { operation_id } => Some(operation_id),
            Self::AvailabilityReported { .. }
            | Self::WorkerReleased { .. }
            | Self::PassArmed { .. }
            | Self::GroupOffered { .. }
            | Self::WorkServiced { .. }
            | Self::PassCompleted { .. } => None,
        }
    }

    /// Returns whether the event records a caller's request rather than the
    /// scheduler's decision or a caller's observation.
    ///
    /// An observer that recomputes the scheduler's answers folds exactly the
    /// requests and the decisions, and nothing a caller concluded.
    #[must_use]
    pub const fn is_request(&self) -> bool {
        matches!(self, Self::Invoked { .. })
    }

    /// Returns whether the event records a terminal outcome a caller observed.
    #[must_use]
    pub const fn is_observation(&self) -> bool {
        matches!(
            self,
            Self::Completed { .. } | Self::Unknown { .. } | Self::NotAdmitted { .. }
        )
    }

    /// Returns the pass a scheduling event belongs to.
    ///
    /// The match is exhaustive rather than defaulted. A catch-all would answer
    /// `None` for a future scheduling event that does belong to a pass, and it
    /// would do so silently; naming every variant makes adding one a decision
    /// the compiler insists on.
    #[must_use]
    pub const fn pass(&self) -> Option<PassIndex> {
        match *self {
            Self::PassArmed { pass, .. }
            | Self::GroupOffered { pass, .. }
            | Self::WorkServiced { pass, .. }
            | Self::PassCompleted { pass, .. } => Some(pass),
            Self::Invoked { .. }
            | Self::Completed { .. }
            | Self::Unknown { .. }
            | Self::NotAdmitted { .. }
            | Self::AvailabilityReported { .. }
            | Self::WorkerReleased { .. } => None,
        }
    }

    /// Returns the tick a scheduling event was recorded at.
    ///
    /// Only the scheduler's own decisions carry a tick, because only they
    /// happen at one. An observer folds these to keep a tick reference of its
    /// own, which is what lets it decide whether a worker occupancy has run
    /// past the cost that opened it.
    #[must_use]
    pub const fn tick(&self) -> Option<TickIndex> {
        match *self {
            Self::AvailabilityReported { tick, .. }
            | Self::WorkerReleased { tick, .. }
            | Self::PassArmed { tick, .. }
            | Self::GroupOffered { tick, .. }
            | Self::PassCompleted { tick, .. } => Some(tick),
            Self::Invoked { .. }
            | Self::Completed { .. }
            | Self::Unknown { .. }
            | Self::NotAdmitted { .. }
            | Self::WorkServiced { .. } => None,
        }
    }
}
