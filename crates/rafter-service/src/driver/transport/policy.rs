#![allow(clippy::wildcard_imports)]

//! Who this driver authorizes, what it retires, and what it will still serve.
//!
//! Split from [`super::control_plane`] along the line between *absorbing* a
//! membership fact and *concluding* something from the state it left.
//! [`super::reconciliation`] owns the first; every derivation here reads state
//! and writes at most the record of what the link layer accepted.
//!
//! **Three sets, and they answer three different questions.** The two runtime
//! facts — `effective_members` and `committed_members` — are what this replica's
//! own runtime last reported, raw. The register — `current_committed` — is the
//! latest *positioned* observation of the committed membership this driver has
//! any evidence of, which can come from a durable record standing ahead of a
//! rebuilt runtime. Authorization is the union of all three; the local replica's
//! own service state is the runtime facts alone.
//!
//! The asymmetry is the eleventh reviewer's finding and it is deliberate. A
//! checkpoint restored beside a lagging runtime names replicas the runtime has
//! not caught up to, and publishing the runtime facts alone put every one of them
//! beneath the retirement floor and outside the peer set — which is the wire
//! definition of *retired*, over identities this driver's own durable evidence
//! calls live. If one of them was the leader, the frames that would have advanced
//! the runtime were the frames being refused, and nothing else was ever going to
//! arrive. So the record's later observation is sufficient authorization for an
//! identity, and Raft validates what the frames actually say.
//!
//! What the union deliberately does not move is [`DriverServiceState`]. A replica
//! its own runtime does not name is receiving no replication, and answering a
//! local read from it is an unboundedly stale view with nothing to bound it. It
//! reports [`DriverServiceState::NotMember`] — the condition that ends by itself
//! — while its identity stays unretired everywhere else.

use std::collections::BTreeSet;

use crate::transport::{AuthenticatedPeerValidator, PeerPolicy, RaftTransport};

use super::super::*;
use super::state::{DesiredPeerPolicy, TransportDriverState};

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

    /// Every replica this driver's own *runtime* named, in either membership
    /// fact.
    ///
    /// The union that keeps a joiner able to speak: a replica added by a change
    /// that has appended and not committed is in the effective half only, and it
    /// has to be able to catch up or the change can never commit. The committed
    /// half is the floor the effective one cannot narrow past.
    ///
    /// Deliberately *not* the authorization set — see
    /// [`TransportDriverState::authorized_members`]. This is what the local
    /// replica's own standing is read from, and what a readmission is counted
    /// over, because both of those questions are about what this replica's
    /// stream says rather than about what any record proves.
    fn named_members(&self) -> BTreeSet<NodeId> {
        self.effective_members
            .union(&self.committed_members)
            .copied()
            .collect()
    }

    /// Every identity this driver may authorize: the two runtime facts, plus the
    /// committed membership its positioned register names.
    ///
    /// **The third term is what a lagging runtime cannot take away.** The
    /// register is the latest observation of the committed membership this driver
    /// has any evidence of, and a durable record legitimately stands ahead of a
    /// rebuilt runtime — a commit index is volatile, and the record's position is
    /// not. Deriving authorization from the runtime facts alone therefore
    /// published a policy that retired every replica the record named and the
    /// runtime had not reached, which is permanent, and which is self-locking
    /// when one of them is the leader.
    ///
    /// It cannot over-authorize. The register's membership is the *live* reading
    /// — every identity a committed removal spent is already filtered out of it —
    /// so this term names only replicas the cluster's own committed history says
    /// are members, and a crossing that removes one takes it out of the register
    /// in the same fold that raises the mark over it.
    fn authorized_members(&self) -> BTreeSet<NodeId> {
        let mut authorized = self.named_members();
        authorized.extend(self.live_committed_members().iter().copied());
        authorized
    }

    /// Installs the authorization policy the group requires, or installs
    /// nothing.
    ///
    /// The retry the transport boundary requires. [`RaftTransport::update_peers`]
    /// is documented as fallible, which makes retry the caller's obligation — and
    /// this driver is the caller. It does not repair itself the way a dropped
    /// frame does: Raft re-sends a lost heartbeat, and nothing re-derives a
    /// policy, because the membership fact that licensed it is already behind
    /// `committed_members`.
    ///
    /// **All or nothing, and now that is the whole of it.** A membership the
    /// validator cannot fully name is not published: a partial peer set
    /// authorizes fewer replicas than the cluster has, which is a quorum-splitting
    /// configuration change made by accident, while leaving the previous policy
    /// in place is merely stale — the floor is monotone, so an older policy
    /// retires fewer identities and never more, and the driver's own inbound
    /// check refuses the retired replica meanwhile.
    ///
    /// This used to be two flushes, and the second one had to be per replica: a
    /// fence was one statement about one replica, so fencing three of four
    /// removed replicas was strictly better than fencing none. A floor makes that
    /// distinction disappear — one statement retires every identity beneath it at
    /// once, and a directory that cannot name some *live* replica no longer
    /// withholds anything about the *removed* ones, because there is nothing
    /// per-removal to withhold.
    ///
    /// Idempotent and cheap when nothing moved: a policy that matches what the
    /// transport accepted makes no call. That is what lets it run from every
    /// entry point rather than from a schedule.
    ///
    /// **Nothing is published while this driver's own inputs contradict each
    /// other.** A retirement floor is permanent, and a permanent statement issued
    /// from facts that disagree is the one mistake no later publication corrects.
    ///
    /// **Exactly one call per membership transaction, and that is the second
    /// half of the same rule.** [`super::reconciliation`] folds every fact of one
    /// batch into a candidate and reaches this once, on a candidate that survived
    /// all of them — so a report whose second event contradicts its first
    /// publishes nothing rather than publishing the first and then refusing.
    ///
    /// **Where else it may run.** Every named entry point of the driver — both
    /// constructors, `tick`, `deliver`, and `drive_pending_reads` — and nowhere
    /// implicit. It is deliberately not called from `DriverShared::lock`, which
    /// would put an embedder's transport calls inside every poll of a client
    /// future, and deliberately not reachable from any `Drop`: reclamation is a
    /// leaf that never waits for this lock, and a flush is neither. Like every
    /// other embedder call this driver makes, it runs with the state lock held,
    /// so a transport that drops a client future inside it reclaims through the
    /// deferred queue exactly as [`super::state::DriverShared::reclaim`]
    /// describes.
    pub(super) fn flush_peer_policy(&mut self) {
        if self.contradiction.is_some() {
            return;
        }
        let desired = self.desired_policy();
        // `NodeId` sets on both sides, comparing a set of replicas against a
        // record of the principals the link layer accepted for them, and it is
        // sound for exactly one reason: a `PeerPrincipal` is stable for the
        // lifetime of its `NodeId`. `AuthenticatedPeerValidator::principal_for_node`
        // states that, so a directory cannot move a live ID to a different
        // principal underneath a published set and leave this comparison
        // reporting level while the transport authorizes the wrong subject.
        // Credential rotation happens *beneath* a principal and is invisible
        // here, which is what makes the stability requirement affordable.
        if self.published_policy.as_ref() == Some(&desired) {
            return;
        }
        let mut principals = Vec::new();
        for node_id in desired.peers.iter().copied() {
            let Some(principal) = self.validator.principal_for_node(&self.group_id, node_id) else {
                self.refused_peer_updates = self.refused_peer_updates.saturating_add(1);
                return;
            };
            principals.push(principal);
        }
        if self
            .transport
            .update_peers(
                &self.group_id,
                PeerPolicy::new(principals, desired.retirement_floor),
            )
            .is_err()
        {
            self.refused_peer_updates = self.refused_peer_updates.saturating_add(1);
            return;
        }
        self.published_policy = Some(desired);
    }

    /// The policy the group currently requires: who may speak, and how far
    /// retirement reaches.
    ///
    /// **The floor is the mark, unchanged and uninterpreted.** Every identity
    /// this group has ever committed is at or below `committed_id_high_water`, so
    /// an identity at or below it that this policy does not authorize is one a
    /// committed removal spent — or one an allocator produced out of order, which
    /// [`TransportDriverState::is_spent`] already refuses for the same reason and
    /// in the same direction.
    fn desired_policy(&self) -> DesiredPeerPolicy {
        DesiredPeerPolicy {
            peers: self.desired_peers(),
            retirement_floor: self.committed_id_high_water,
        }
    }

    /// The peer set the group currently requires, which is everything it
    /// authorizes less the local node and less every identity a committed
    /// removal has spent.
    ///
    /// The spent exclusion is the fail-closed reading of a violated single-use
    /// contract. A `(group_id, NodeId)` pair is retired by a committed removal
    /// and is never validly re-added, but the kernel keeps no tombstones and
    /// cannot refuse the re-addition after compaction has erased the history it
    /// would need — so this driver can be handed a committed membership naming a
    /// replica this group has already retired. Leaving it out of the peer set is
    /// the only answer the boundary can carry: publishing the ID would ask the
    /// link layer to authorize a principal beneath its own retirement floor,
    /// which is a policy no directory can honor and a driver that believed it had
    /// would report itself level while the replica stayed silent.
    ///
    /// The local node is excluded here rather than stored excluded: a peer set
    /// names who may speak *to* this node, and a node is not a peer of itself.
    /// Derived on each attempt, so an incarnation adopted under a different node
    /// ID excludes the right replica without anything having to notice.
    fn desired_peers(&self) -> BTreeSet<NodeId> {
        self.authorized_members()
            .into_iter()
            .filter(|node_id| *node_id != self.node_id && !self.is_spent(*node_id))
            .collect()
    }

    /// Whether the transport's policy is behind the one the group requires.
    pub(super) fn peer_policy_is_stale(&self) -> bool {
        self.published_policy.as_ref() != Some(&self.desired_policy())
    }

    /// How many spent identities this replica's own runtime names again.
    ///
    /// Zero for every cluster that keeps the single-use contract, which is what
    /// makes it worth reading. A non-zero value is one specific violation and
    /// not a link-layer condition: some replica was named again under a `NodeId`
    /// a committed removal had already spent, and this driver is refusing it —
    /// out of the published peer set, out of the inbound check, and beneath the
    /// retirement floor its own policy states.
    ///
    /// Counted over the *raw* runtime facts rather than over what this driver
    /// authorizes, and that is two separate choices. The raw committed
    /// configuration is stored at all so the violating ID is still there to see —
    /// the register has it filtered out, so counting there would report zero for
    /// exactly the case this exists to name. And the register is left out of the
    /// count because it can name no spent identity by construction, so including
    /// it could only ever dilute what the number means: this counts what *the
    /// cluster told this replica*, which is the thing an operator can act on.
    ///
    /// Current state rather than history, like
    /// [`super::TransportRaftDriver::peer_policy_is_stale`] and unlike
    /// `refused_peer_updates`: it falls back to zero only if a later membership
    /// stops naming the spent replica, which no correct deployment needs and no
    /// incorrect one is helped by.
    pub(super) fn readmitted_retired_peers(&self) -> usize {
        self.named_members()
            .into_iter()
            .filter(|node_id| self.is_spent(*node_id))
            .count()
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
    /// happens next; a released driver is reported before anything derived from
    /// a group it does not hold; either contradiction outranks both conclusions
    /// drawn from the membership facts, because it says those facts cannot be
    /// trusted; and decommissioning outranks the condition that ends, because a
    /// rollback can be re-proposed and a spent identity cannot.
    pub(super) fn service_state(&self) -> DriverServiceState {
        if self.shutting_down {
            return DriverServiceState::ShuttingDown;
        }
        if self.group.is_none() {
            return DriverServiceState::Released;
        }
        if let Some(contradiction) = self.contradiction {
            return contradiction.service_state();
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
