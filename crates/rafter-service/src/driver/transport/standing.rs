//! Whether an identity is spent, and what this driver will still serve.
//!
//! Split from [`super::policy`] along the line between what this driver states
//! *about the cluster* and what it concludes *about itself*. The spent test is
//! here because both questions read it: a peer set excludes what a committed
//! removal consumed, and so does this replica's own standing. Nothing here
//! writes any state.

#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

use crate::transport::{AuthenticatedPeerValidator, RaftTransport};

use super::state::TransportDriverState;
use super::*;

impl<G, A, R, T, V> TransportDriverState<G, A, R, T, V>
where
    G: Clone + Ord + Debug + Send + Sync + 'static,
    A: ReplicatedStateMachine + Send + 'static,
    A::Command: Send + 'static,
    A::CommandResult: Clone + Send + 'static,
    A::Query: Clone + Send + 'static,
    A::QueryResult: Send + 'static,
    R: PersistedRaftRuntime + Send + 'static,
    T: RaftTransport<G>,
    V: AuthenticatedPeerValidator<G, T::PeerPrincipal> + Send + Sync + 'static,
{
    /// Whether a committed removal has already consumed this `(group, NodeId)`.
    ///
    /// Two reads and no set. Under the allocation contract [`rafter::NodeId`]
    /// states — every newly admitted ID exceeds every ID the group has ever
    /// committed — the IDs that have ever been committed are exactly those at or
    /// below `committed_id_high_water`, so one of them that the live committed
    /// configuration no longer names is one a committed removal spent.
    ///
    /// That replaces a set of every removal the driver ever saw, which grew
    /// without bound over the life of a long-running group and had no retention
    /// policy — the same unbounded-tombstone structure the kernel declined to
    /// keep, moved one layer up and no more affordable there.
    ///
    /// Before any committed configuration has been observed there is no mark and
    /// nothing is spent, which is why the mark is an `Option` rather than a
    /// zero: `NodeId(0)` is a legal identity and cannot double as "none".
    ///
    /// The deployment-visible consequence is that an allocation *gap* below the
    /// mark is unallocatable. That is not a rounding error in the derivation, it
    /// is the contract said out loud: fresh means greater than anything ever
    /// committed. A deployment that allocates non-monotonically has its "fresh"
    /// IDs refused here, which is the fail-closed direction — the alternative
    /// reads a violated precondition as permission.
    pub(super) fn is_spent(&self, node_id: NodeId) -> bool {
        self.committed_id_high_water
            .is_some_and(|mark| node_id <= mark)
            && !self.live_committed_members().contains(&node_id)
    }

    /// Whether `node_id` may speak to this driver at all.
    ///
    /// The inbound admission check, and it is deliberately the same set the peer
    /// set is derived from — including the register, for the reason
    /// [`TransportDriverState::authorized_members`] gives. A driver that
    /// published a replica as authorized and then refused its frames at its own
    /// door would be running two admission policies that disagree, and the one
    /// that disagreed would be the one that decides whether this replica ever
    /// catches up.
    ///
    /// Spent-ness outranks all three terms, and the order is the whole point. A
    /// committed removal spends the `(group_id, NodeId)` pair, so a later fact
    /// naming that ID is not evidence that the replica may speak again — it is
    /// evidence that the contract was broken, and the frame is refused whatever
    /// the fact says. The alternative reads a violated precondition as
    /// permission, and would admit exactly the replica this driver's own
    /// published policy retires.
    ///
    /// Asks each set rather than building their union, because this runs on
    /// every inbound frame and the union is the same answer with an allocation
    /// in front of it.
    pub(super) fn is_admitted(&self, node_id: NodeId) -> bool {
        !self.is_spent(node_id)
            && (self.effective_members.contains(&node_id)
                || self.committed_members.contains(&node_id)
                || self.live_committed_members().contains(&node_id))
    }

    /// Whether this replica's own runtime still names it.
    ///
    /// **Deliberately narrower than [`TransportDriverState::is_admitted`], and
    /// the register is what it leaves out.** The two answer different questions.
    /// Admission asks whether an *identity* may speak, which a durable record
    /// standing ahead of a lagging runtime is sufficient evidence for. This asks
    /// whether *this process* is being replicated to, which only its own runtime
    /// can answer — and a replica whose runtime does not name it is receiving
    /// nothing, so answering a local read from it is an unboundedly stale view
    /// with nothing to bound it.
    fn runtime_names_local_replica(&self) -> bool {
        self.effective_members.contains(&self.node_id)
            || self.committed_members.contains(&self.node_id)
    }

    /// Whether a committed removal has spent this driver's own identity.
    ///
    /// The local replica is retired by the same fact and the same diff as any
    /// peer, so this needs no separate record: `node_id` simply stops being in
    /// the live committed configuration, and the spent test answers.
    pub(super) fn is_decommissioned(&self) -> bool {
        self.is_spent(self.node_id)
    }

    /// Why this driver is refusing new client work, if it is.
    ///
    /// **Ordered by what a supervisor can still do about it**, most terminal
    /// first. Shutdown outranks everything because nothing else changes what
    /// happens next. Either contradiction comes next and outranks `Released`,
    /// because both are terminal for the incarnation and releasing the group
    /// does not resolve the fork — a driver that reported `Released` after a
    /// contradiction would read as an ordinary reusable driver, and the next
    /// adoption would rearm state whose membership facts cannot be trusted.
    /// A released driver is then reported before anything derived from a group
    /// it does not hold; and decommissioning outranks the condition that ends,
    /// because a rollback can be re-proposed and a spent identity cannot.
    pub(super) fn service_state(&self) -> DriverServiceState {
        if self.shutting_down {
            return DriverServiceState::ShuttingDown;
        }
        if let Some(contradiction) = self.contradiction {
            return contradiction.service_state();
        }
        if self.group.is_none() {
            return DriverServiceState::Released;
        }
        if self.is_decommissioned() {
            return DriverServiceState::Decommissioned {
                node_id: self.node_id,
            };
        }
        // Not the negation of decommissioning, and not the negation of
        // `is_admitted` either. A replica that was never named and one that was
        // removed both fail this; only the second is a retirement, and the first
        // ends by itself. A replica an ahead-of-runtime record still names is
        // *admitted* everywhere and reported here, because what it is waiting for
        // is its own runtime.
        if !self.runtime_names_local_replica() {
            return DriverServiceState::NotMember {
                node_id: self.node_id,
            };
        }
        DriverServiceState::Serving
    }

    /// Refuses a client operation this driver's own state says it must not
    /// start.
    ///
    /// Every refusal is a `NotAppended`-shaped fact: nothing was proposed, so
    /// nothing is in flight to be uncertain about. None is reported as a group
    /// failure, because the group is fine — what is wrong is this replica's
    /// standing in the cluster, its link layer's, or this driver's own.
    pub(super) fn reject_if_not_serving(&self) -> Result<(), DriverUnavailableReason> {
        match self.service_state() {
            DriverServiceState::Serving => Ok(()),
            DriverServiceState::Decommissioned { .. } => {
                Err(DriverUnavailableReason::Decommissioned)
            }
            DriverServiceState::NotMember { .. } => Err(DriverUnavailableReason::NotMember),
            DriverServiceState::ContradictoryCurrentState { .. } => {
                Err(DriverUnavailableReason::ContradictoryCurrentState)
            }
            DriverServiceState::ContradictoryTransitionPredecessor { .. } => {
                Err(DriverUnavailableReason::ContradictoryTransitionPredecessor)
            }
            DriverServiceState::Released => Err(DriverUnavailableReason::Released),
            // Every client surface refuses shutdown ahead of this call with its
            // own older variant, so this arm is the projection staying total
            // rather than a path a client reaches; see
            // [`DriverUnavailableReason::ShuttingDown`].
            DriverServiceState::ShuttingDown => Err(DriverUnavailableReason::ShuttingDown),
        }
    }
}
