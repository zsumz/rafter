#![allow(clippy::wildcard_imports)]

//! What this driver tells its link layer about who may speak.
//!
//! Split from [`super::state`] along the line that file's own header draws:
//! that one answers "what does a step do", and this one answers "who is allowed
//! to send one". Everything between a committed configuration and the two
//! statements the transport is owed for it — a peer set and a fence — is here,
//! and the step loop reaches it through one call.
//!
//! The state these derivations read still lives on
//! [`super::state::TransportDriverState`], because the step loop's fields and
//! these are one struct behind one lock. What moved is every rule that reads
//! them.

use std::collections::BTreeSet;

use crate::transport::{AuthenticatedPeerValidator, PeerSet, RaftTransport};

use super::super::*;
use super::state::TransportDriverState;

/// The membership fact one publication is derived from.
///
/// A fact rather than a set plus a decision, and that is the whole point of the
/// type. Publishing answers two questions — which principals the link layer may
/// authorize, and which it must fence — and both are licensed by the same one
/// fact: what the cluster has *committed*. A caller that supplied a set and a
/// fencing flag as separate arguments could answer the two inconsistently, and
/// one did: adoption published a narrowed peer set for a committed removal and
/// withheld the fence for it, because the two travelled apart. Here they cannot.
///
/// So every publisher names what it knows, and
/// [`TransportDriverState::publish_membership`] derives both answers from it.
pub(super) enum MembershipFact {
    /// A configuration that is effective and may still be uncommitted.
    ///
    /// It may only widen. A replica joining under joint consensus has to be able
    /// to speak before the change commits, or it can never catch up and the
    /// change can never commit; and an uncommitted change can still be reverted,
    /// so nothing may be taken away for it.
    Effective(BTreeSet<NodeId>),
    /// A committed configuration, and the effective one beside it.
    ///
    /// Both halves are load-bearing and neither stands alone. `committed` is the
    /// only fact that licenses narrowing the set and fencing what left it.
    /// `effective` is what keeps an in-flight change's joiner able to speak
    /// across the same publication — a replica that rebuilt its runtime from
    /// durable storage can hold an appended-but-uncommitted addition in its log,
    /// and publishing the committed set alone would take the joiner's
    /// authorization away and stall the change that needs it.
    Committed {
        committed: BTreeSet<NodeId>,
        effective: BTreeSet<NodeId>,
    },
}

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
    /// Keeps the transport's peer set level with the group's membership.
    ///
    /// `Appended` carries the effective configuration and `Applied` carries the
    /// committed one — that is how `rafter-app` builds them — so each arm names
    /// the fact it has and [`TransportDriverState::publish_membership`] decides
    /// what the fact licenses. The `Applied` arm reads the effective membership
    /// beside it for the reason [`MembershipFact::Committed`] gives: a change
    /// committing does not retract a *later* change already appended over it.
    pub(super) fn route_membership_event(&mut self, event: &MembershipEvent<G>) {
        match event {
            MembershipEvent::Appended { membership, .. } => {
                let effective = membership.replica_ids().into_iter().collect();
                self.publish_membership(MembershipFact::Effective(effective));
            }
            MembershipEvent::Applied { membership, .. } => {
                let committed = membership.replica_ids().into_iter().collect();
                // A driver holding no group contributes no widening and still
                // honors the committed fact: an absent effective membership must
                // not turn a fence into a silence.
                let effective = self.effective_members().unwrap_or_default();
                self.publish_membership(MembershipFact::Committed {
                    committed,
                    effective,
                });
            }
            // A rejected change never entered the log, and a variant this driver
            // does not know is not a membership fact it can act on.
            _ => {}
        }
    }

    /// Records what one membership fact requires of the link layer, then tries
    /// to install it.
    ///
    /// Two statements, derived from one fact: which principals the transport may
    /// authorize, and which it must fence. Neither may be skipped because the
    /// other could not be made — a membership event that both narrows the set
    /// and licenses a fence installs two admission controls, and a driver that
    /// dropped one because the other failed would leave a committed-removed
    /// replica able to speak.
    ///
    /// No caller chooses between them. The set derived is a superset of the
    /// committed membership, and the replicas fenced are those the driver knew
    /// before that this superset no longer names, less the local node — so a
    /// fenced replica is always absent from the committed membership, which is
    /// the only thing that licenses fencing it. The local exclusion only narrows
    /// the fence set, so it cannot reach a replica the committed membership
    /// still names. Both properties are consequences of the derivation below
    /// rather than obligations on a caller: a caller that supplies
    /// [`MembershipFact::Effective`] cannot narrow or fence, and one that
    /// supplies [`MembershipFact::Committed`] cannot narrow past what committed.
    ///
    /// **Recording is separate from installing, and that separation is the
    /// contract.** This method derives obligations; it does not decide that they
    /// were met. `known_members` advances here unconditionally, because it is
    /// the record of what the *cluster* says and the next committed removal has
    /// to be computed against the membership the cluster had. The record of what
    /// the *link layer* took lives in `published_peers` and `pending_fences`, and
    /// only [`TransportDriverState::flush_peer_control_plane`] moves those — on
    /// an `Ok` from the transport and on nothing else. A driver with one piece of
    /// state for both facts forgets a refused fence the instant it advances,
    /// because there is no later event to re-derive it from: the removal is
    /// already behind `known_members`.
    ///
    /// The retraction rule is the mirror of the licensing rule, and holds for
    /// the same reason. Only the *committed* half of a `Committed` fact clears an
    /// outstanding fence, because only a committed fact could have created one. A
    /// replica the cluster has committed back into the membership is no longer a
    /// removed peer and must not be fenced; a replica named only by an effective
    /// configuration is named by a change that may still revert, and a fact too
    /// weak to create the obligation is too weak to retract it.
    fn publish_membership(&mut self, fact: MembershipFact) {
        let (members, committed) = match fact {
            // Union with what is already known, never a replacement: an
            // uncommitted change may be reverted, so it may add authorization
            // and may not take any away — and, by the same token, retracts no
            // fence, which is why it contributes no committed half.
            MembershipFact::Effective(effective) => {
                let mut widened = self.known_members.clone();
                widened.extend(effective);
                (widened, BTreeSet::new())
            }
            // Union of the two, so the committed half sets the floor the set may
            // not narrow past and the effective half adds whatever a change in
            // flight needs on top of it.
            MembershipFact::Committed {
                committed,
                effective,
            } => {
                let mut members = committed.clone();
                members.extend(effective);
                (members, committed)
            }
        };
        let removed = self
            .known_members
            .difference(&members)
            .copied()
            .filter(|node_id| *node_id != self.node_id);
        self.pending_fences.extend(removed);
        // Disjoint from the extension above — `removed` is what the union no
        // longer names and `committed` is contained in it — so this only ever
        // retracts an obligation an *earlier* fact left owed.
        self.pending_fences
            .retain(|node_id| !committed.contains(node_id));
        self.known_members = members;
        self.flush_peer_control_plane();
    }

    /// Installs everything the link layer still owes the group, and leaves owed
    /// whatever it refuses.
    ///
    /// The retry the transport boundary requires. Both
    /// [`RaftTransport::update_peers`] and [`RaftTransport::fence_peer`] are
    /// documented as fallible, which makes retry the caller's obligation — and
    /// this driver is the caller. Neither statement repairs itself the way a
    /// dropped frame does: Raft re-sends a lost heartbeat, and nothing
    /// re-derives a peer set or a fence, because the membership fact that
    /// licensed them is already behind `known_members`.
    ///
    /// Idempotent and cheap when there is nothing owed: a peer set that matches
    /// what the transport accepted makes no call, and an empty obligation set
    /// makes no call. That is what lets it run from every entry point rather
    /// than from a schedule.
    ///
    /// **Where it may run.** Every named entry point of the driver — both
    /// constructors, `tick`, `deliver`, and `drive_pending_reads` — and nowhere
    /// implicit. It is deliberately not called from `DriverShared::lock`, which
    /// would put an embedder's transport calls inside every poll of a client
    /// future, and deliberately not reachable from any `Drop`: reclamation is a
    /// leaf that never waits for this lock, and a flush is neither. Like every
    /// other embedder call this driver makes, it runs with the state lock held,
    /// so a transport that drops a client future inside it reclaims through the
    /// deferred queue exactly as [`super::state::DriverShared::reclaim`]
    /// describes.
    pub(super) fn flush_peer_control_plane(&mut self) {
        self.flush_peer_set();
        self.flush_pending_fences();
    }

    /// Publishes the peer set the group requires, or publishes nothing.
    ///
    /// All or nothing. A membership the validator cannot fully name is not
    /// published at all: a partial peer set authorizes fewer replicas than the
    /// cluster has, which is a quorum-splitting configuration change made by
    /// accident, while leaving the previous set in place is merely stale. That
    /// last clause is true of the peer set and only of the peer set — a fence
    /// the same fact licensed is not stale when it is withheld, it is absent,
    /// which is why fencing is not part of this decision.
    ///
    /// Staleness is now bounded rather than merely tolerable, and that is what
    /// changed. The previous set stays in place until the *next* attempt, and
    /// the next attempt is the driver's next entry point rather than the
    /// cluster's next configuration change. `published_peers` is what makes the
    /// difference observable: it advances only on an accepted publication, so
    /// "the link layer is behind the group" is a state this driver can see and
    /// report rather than an event it counted once.
    ///
    /// Both a principal that cannot be named and a transport refusal are
    /// counted, for the reason they always were, and both now leave the work
    /// outstanding as well as counted.
    ///
    /// The local replica is not in its own peer set: a `PeerSet` names who may
    /// speak *to* this node, and a node is not a peer of itself. Derived on each
    /// attempt rather than stored, so an incarnation adopted under a different
    /// node ID excludes the right replica without anything having to notice.
    fn flush_peer_set(&mut self) {
        let desired = self.desired_peers();
        if self.published_peers.as_ref() == Some(&desired) {
            return;
        }
        let mut principals = Vec::new();
        for node_id in desired.iter().copied() {
            let Some(principal) = self.validator.principal_for_node(&self.group_id, node_id) else {
                self.refused_peer_updates = self.refused_peer_updates.saturating_add(1);
                return;
            };
            principals.push(principal);
        }
        if self
            .transport
            .update_peers(&self.group_id, PeerSet::new(principals))
            .is_err()
        {
            self.refused_peer_updates = self.refused_peer_updates.saturating_add(1);
            return;
        }
        self.published_peers = Some(desired);
    }

    /// Fences every replica a committed removal left owed, and keeps owing the
    /// rest.
    ///
    /// Per replica rather than all or nothing, because the two statements have
    /// different shapes. A peer set is one statement about a whole cluster, and
    /// a partial one authorizes a quorum-splitting subset of it. A fence is one
    /// statement about one replica, and fencing three of four removed replicas
    /// is strictly better than fencing none of them. The set is drained one
    /// entry at a time for the same reason: one replica's refusal is not
    /// another's.
    ///
    /// A replica this deployment cannot name **stays owed** rather than being
    /// discarded as counted. `principal_for_node` answering `None` is a
    /// statement about the directory, not about the cluster: a deployment that
    /// cannot name a replica today can name it once its directory catches up,
    /// and the removal it could not act on is exactly as committed either way.
    /// Dropping the obligation there would make an unnameable removal
    /// permanently unfenced — the same forgetting as a refused fence, arriving
    /// through the validator instead of the transport.
    ///
    /// The local node is excluded here as well as where the obligation is
    /// recorded, and is dropped rather than skipped. `node_id` changes at
    /// adoption, so an obligation recorded against a replica that this
    /// incarnation has since *become* is not a removed peer any more, and a
    /// driver that fenced it would fence itself.
    fn flush_pending_fences(&mut self) {
        for node_id in self.pending_fences.iter().copied().collect::<Vec<_>>() {
            if node_id == self.node_id {
                self.pending_fences.remove(&node_id);
                continue;
            }
            let Some(principal) = self.validator.principal_for_node(&self.group_id, node_id) else {
                self.refused_peer_updates = self.refused_peer_updates.saturating_add(1);
                continue;
            };
            if self
                .transport
                .fence_peer(&self.group_id, principal)
                .is_err()
            {
                self.refused_peer_updates = self.refused_peer_updates.saturating_add(1);
                continue;
            }
            self.pending_fences.remove(&node_id);
        }
    }

    /// The peer set the group currently requires, which is its membership less
    /// the local node.
    fn desired_peers(&self) -> BTreeSet<NodeId> {
        self.known_members
            .iter()
            .copied()
            .filter(|node_id| *node_id != self.node_id)
            .collect()
    }

    /// Whether the transport's peer set is behind the one the group requires.
    pub(super) fn peer_set_is_stale(&self) -> bool {
        self.published_peers.as_ref() != Some(&self.desired_peers())
    }

    /// Whether this driver's membership names `node_id`.
    ///
    /// The inbound admission check, and it is deliberately the same set the peer
    /// set is derived from. `known_members` is committed ∪ effective, so it
    /// names every replica that may legitimately speak — including one added by
    /// a change that has appended and not committed, which has to be able to
    /// speak before the change commits or the change can never commit.
    pub(super) fn is_member(&self, node_id: NodeId) -> bool {
        self.known_members.contains(&node_id)
    }

    /// Reads the group's effective membership, or `None` if it holds no group.
    fn effective_members(&self) -> Option<BTreeSet<NodeId>> {
        self.group.as_ref().map(|group| {
            group
                .runtime()
                .membership()
                .replica_ids()
                .into_iter()
                .collect()
        })
    }

    /// Publishes the adopted group's membership, so the transport's peer set is
    /// defined from adoption rather than from the first change this incarnation
    /// happens to observe.
    ///
    /// A committed fact, because an adoption is where a *change* can be observed
    /// without any event announcing it. The supervisor pattern this driver
    /// documents is release, rebuild the runtime from durable storage, adopt:
    /// the committed membership a rebuilt runtime reports can have advanced past
    /// a removal while the driver held no group, and no `Applied` will ever be
    /// emitted for it because the change committed elsewhere. The driver still
    /// holds its own `known_members` from before the release, so the difference
    /// is there to be taken — and taking it is the only thing that makes one
    /// committed removal mean the same at adoption as it does on a routed event.
    ///
    /// The effective membership travels with it rather than instead of it. A
    /// runtime rebuilt from durable storage can hold an appended-but-uncommitted
    /// change, which makes its effective membership *narrower* than its
    /// committed one for a removal in flight; publishing that would take
    /// authorization away for a change that may still revert, with nothing left
    /// to repair it — no `Applied` fires, because committed never moved, and no
    /// `Appended` fires, because this driver has no input that carries a
    /// membership request.
    ///
    /// Adoption also discharges whatever the previous incarnation left owed, and
    /// gets that for free rather than by arrangement: publishing runs the flush,
    /// and the obligations are the driver's rather than the group's, so a
    /// release does not cancel them. That is the half a re-derivation cannot
    /// cover — by the time the driver holds no group, `known_members` has
    /// already moved past any removal it observed, so the difference this method
    /// takes is empty and the fence is owed rather than derivable.
    ///
    /// A driver holding no group publishes nothing and still owes what it owed.
    /// The early return skips the *derivation*, which needs a runtime; it does
    /// not discard obligations, which need only the next entry point.
    pub(super) fn publish_adopted_membership(&mut self) {
        let Some(group) = self.group.as_ref() else {
            return;
        };
        let runtime = group.runtime();
        let committed = runtime
            .committed_membership()
            .replica_ids()
            .into_iter()
            .collect();
        let effective = runtime.membership().replica_ids().into_iter().collect();
        self.publish_membership(MembershipFact::Committed {
            committed,
            effective,
        });
    }
}

#[cfg(test)]
#[path = "control_plane/tests.rs"]
mod tests;
