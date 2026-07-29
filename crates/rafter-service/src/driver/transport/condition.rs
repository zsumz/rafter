//! What condition one driver is in, and why it refuses.
//!
//! Split from [`super::state`] along the line that file's own header draws: that
//! one answers "what does a step do", and everything here answers "what is this
//! driver's standing, and what does a client hear about it". Neither type below
//! reads driver state or writes any — [`super::policy`] derives them, and
//! [`super::reconciliation`] records the one of them that is terminal.
//!
//! They live together because they are two views of one question.
//! [`DriverServiceState`] is the whole answer a supervisor polls, over every
//! condition including the ones that have nothing to do with the control plane;
//! [`Contradiction`] is the *stored* half of the two conditions that do, kept
//! apart because the driver has to remember which one it found and where, and
//! because only that pair is terminal, durable, and freezes the record.

use super::super::*;

/// Why a driver is refusing new client work, if it is.
///
/// **A total answer to that question**, which it did not used to be: a released
/// driver and a shut-down one both reported `Serving` while refusing everything,
/// so a supervisor polling this could not tell "ready" from "gone". Every state
/// in which this driver refuses a client operation for a reason of its own is
/// named here.
///
/// One shape, because a client asking "may I write" needs one answer and an
/// operator asking "why not" needs the reason beside it. These are states rather
/// than counts, like [`TransportRaftDriver::peer_policy_is_stale`] and unlike the
/// refusal counters: they say what is true now.
///
/// **Two of them end and four do not.** [`DriverServiceState::NotMember`]
/// clears when the cluster names this replica again, and
/// [`DriverServiceState::Released`] ends at the next adoption.
/// [`DriverServiceState::ContradictoryCurrentState`],
/// [`DriverServiceState::ContradictoryTransitionPredecessor`],
/// [`DriverServiceState::Decommissioned`] and
/// [`DriverServiceState::ShuttingDown`] are terminal for the incarnation.
///
/// None but shutdown stops the protocol. A driver in any of them still ticks,
/// still delivers, still applies what commits, and — except in the two
/// contradictory states, where it deliberately publishes nothing and freezes its
/// durable record — still flushes its peer policy. What stops is admitting *new*
/// client operations. A replica that stopped stepping could not finish the
/// catch-up that ends one of these conditions, and could not stay a useful
/// follower through the others.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum DriverServiceState {
    /// The driver is serving clients.
    ///
    /// Requires that a committed removal has not spent this replica's identity
    /// **and** that some configuration this driver knows still names it. The
    /// second clause is not implied by the first: an addition that appended and
    /// was then truncated back off the log leaves this replica in no
    /// configuration at all with nothing spent, which is
    /// [`DriverServiceState::NotMember`].
    Serving,
    /// A committed configuration change removed this driver's own replica.
    ///
    /// Terminal for this incarnation and not for the driver. The group stays
    /// until [`TransportRaftDriver::release_group`] — the durable log is still
    /// there and the runtime is still live, so a replica that is stepping down
    /// can still be read from and can still help others catch up — and the
    /// supervisor's move is release, then adopt a *fresh* identity. Adopting the
    /// same one back is refused, because the cluster spent it.
    ///
    /// Outranks every non-terminal state when several hold: a backlog drains and
    /// a rollback can be re-proposed, and a removal can be neither.
    Decommissioned {
        /// Local identity spent by the committed removal.
        node_id: NodeId,
    },
    /// No configuration this driver knows names this replica, and no committed
    /// removal spent it either.
    ///
    /// Distinct from [`DriverServiceState::Decommissioned`] in both direction and
    /// permanence, and the difference is the point. A local replica that joined
    /// effectively and was then rolled back — a new leader truncating the
    /// uncommitted addition back off the log — is in no configuration, has an
    /// unspent ID, and is receiving no replication. Reporting it as serving let
    /// it answer local reads from a replica the cluster is not replicating to,
    /// which is an unboundedly stale view with nothing to bound it: exactly the
    /// hazard [`crate::ReadConsistency::Local`] cannot detect on its own. It
    /// covers construction around an unnamed ID too, which is a legitimate
    /// starting point for a fresh joiner whose addition has not committed.
    ///
    /// **Writes and both read levels are refused; ticks and deliveries are
    /// not.** The replica must be able to catch up if the change is re-proposed,
    /// or if it is a joiner whose addition is still in flight, and it cannot do
    /// that without stepping.
    ///
    /// It clears by itself the moment a configuration names the ID again.
    NotMember {
        /// Local identity absent from every known configuration.
        node_id: NodeId,
    },
    /// Two observations of the committed membership at `through` disagree about
    /// it.
    ///
    /// **The committed membership at one log index is one set**, so this is not
    /// two readings to reconcile: it is a durable record and a runtime — or two
    /// durable records — making incompatible claims about a single fact, after
    /// every identity either side has proven spent has been taken out of both.
    /// A cluster that readmits a retired identity does *not* land here; that is a
    /// counted contract violation with an answer of its own, at
    /// [`TransportRaftDriver::readmitted_retired_peers`].
    ///
    /// Terminal for the incarnation, because there is no later fact that decides
    /// it and because the statement at stake is permanent: this driver publishes
    /// a retirement floor to its link layer, and a floor issued from
    /// contradictory inputs either retires a live replica or fails to retire a
    /// removed one, neither of which a later publication takes back. So the
    /// driver refuses client work and publishes nothing while it keeps stepping.
    ///
    /// Reachable after the group is installed, and on a restart that restored a
    /// record carrying the durable contradiction marker — see
    /// [`PeerControlPlaneCheckpoint::contradicted_at`]. An adoption or a
    /// construction that can see a *fresh* disagreement up front refuses with
    /// [`ManagedDriverError::InvalidControlPlaneCheckpoint`] instead, and installs
    /// no group. The supervisor's move either way is
    /// [`TransportRaftDriver::release_group`] and a deliberate reseed.
    ContradictoryCurrentState {
        /// Log position at which the observations disagree.
        through: LogIndex,
    },
    /// A committed transition declares a predecessor this driver's register at
    /// `through` is not.
    ///
    /// **The same terminal condition proved by stronger evidence.** A crossing
    /// carries the membership the kernel computed as standing immediately before
    /// its own entry, and the register one position below it is a claim about
    /// that same committed configuration — with no index between them for the
    /// difference to have happened at. So this is not two observations that might
    /// each be damaged: it is the log's own account of its own history
    /// contradicting a durable record, which is the one-chain contract broken
    /// with the log as the witness.
    ///
    /// Reported apart from [`DriverServiceState::ContradictoryCurrentState`]
    /// because an operator reading it is looking at a different artifact — the
    /// record beside this log, rather than either of two observations. It is
    /// terminal on identical terms, and every refusal it produces is the same.
    ContradictoryTransitionPredecessor {
        /// Position whose membership the committed transition contradicts.
        through: LogIndex,
    },
    /// The driver released its group and has not adopted another.
    ///
    /// Reported rather than folded into `Serving`, which is what it used to be:
    /// every client operation was already refused in this state, and the one
    /// surface a supervisor polls to decide whether to route here said the
    /// replica was fine. Ends at [`TransportRaftDriver::adopt_group`].
    Released,
    /// [`crate::DriverCommandSender::shutdown`] has run, which is terminal.
    ///
    /// The driver refuses every operation including adoption; a supervisor that
    /// wants to serve again builds a driver. Outranks every other state, because
    /// nothing this driver could otherwise report changes what happens next.
    ShuttingDown,
}

/// Which contradiction retired this incarnation, and the position it is about.
///
/// **Two ways for one driver's licensing inputs to be provably inconsistent, and
/// they diagnose different artifacts.** A record and a runtime colliding at one
/// position could be either side's damage; a transition colliding with the
/// register one below it is the log's own account of its own history, computed
/// where the chronology was known, disagreeing with a durable record — so the
/// record is what is wrong. Both are terminal for the incarnation and neither is
/// a counter: there is no later fact that decides one.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Contradiction {
    /// Two observations of the committed membership at `through` disagree.
    CurrentState { through: LogIndex },
    /// The transition immediately above `through` declares a predecessor the
    /// register at `through` is not.
    TransitionPredecessor { through: LogIndex },
}

impl Contradiction {
    /// Reads one refusal as a contradiction, or `None` when it is not one.
    ///
    /// **Only the two terminal shapes are here.** The rest are refusals of an
    /// *input* — a damaged record, a foreign group, a record older than what this
    /// driver holds — and every one of them is raised where a caller can still be
    /// told, so treating them as contradictions would report a driver as sick for
    /// a file it declined to open.
    pub(super) const fn of(reason: ControlPlaneCheckpointError) -> Option<Self> {
        match reason {
            ControlPlaneCheckpointError::ContradictoryCurrentState { through } => {
                Some(Self::CurrentState { through })
            }
            ControlPlaneCheckpointError::ContradictoryTransitionPredecessor { through } => {
                Some(Self::TransitionPredecessor { through })
            }
            _ => None,
        }
    }

    /// The contradiction a *restored* marker describes.
    ///
    /// **A record carries the position and not which comparison found it**, and
    /// that narrowing is deliberate. What a restored marker has to establish is
    /// that this chain observed an unresolved fork at a position, which is what
    /// decides whether the driver may serve; which of two comparisons produced it
    /// is a diagnosis the process that found it already emitted, and a durable
    /// field for it would be one more thing a hand-edited file can lie about
    /// without changing the answer.
    pub(super) const fn restored(through: LogIndex) -> Self {
        Self::CurrentState { through }
    }

    /// The position the two claims disagree about.
    pub(super) const fn through(self) -> LogIndex {
        match self {
            Self::CurrentState { through } | Self::TransitionPredecessor { through } => through,
        }
    }

    /// How a supervisor polling [`DriverServiceState`] hears about it.
    pub(super) const fn service_state(self) -> DriverServiceState {
        match self {
            Self::CurrentState { through } => {
                DriverServiceState::ContradictoryCurrentState { through }
            }
            Self::TransitionPredecessor { through } => {
                DriverServiceState::ContradictoryTransitionPredecessor { through }
            }
        }
    }

    /// The typed refusal an entry point that can still return one raises.
    pub(super) const fn refusal(self) -> ControlPlaneCheckpointError {
        match self {
            Self::CurrentState { through } => {
                ControlPlaneCheckpointError::ContradictoryCurrentState { through }
            }
            Self::TransitionPredecessor { through } => {
                ControlPlaneCheckpointError::ContradictoryTransitionPredecessor { through }
            }
        }
    }
}
