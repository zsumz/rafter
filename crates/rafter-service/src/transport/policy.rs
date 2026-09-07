//! The one statement this crate makes to a link layer about admission.
//!
//! Who may speak and how far retirement reaches travel in a single value,
//! because they are licensed by the same committed fact and a caller that could
//! publish them apart could publish them inconsistently. Nothing here derives a
//! policy; it is the shape a driver hands over and a transport installs whole.

#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

use super::*;

/// Who may speak for a group, and which identities are retired.
///
/// **One statement rather than a set plus a stream of per-principal fences**, and
/// that is the whole design. Retiring a replica used to be an operation —
/// `fence_peer`, once per removal, permanent, and owed until the link layer took
/// it — which meant a driver had to *remember which removals it had already
/// acted on*. That memory was a bounded set with an unbounded question attached:
/// "has this identity's fence been made" is not the same question as "may this
/// identity be admitted again", and one bit answered both. An exact committed
/// removal arriving beneath a later observation was suppressed as
/// already-handled, and the join that merged two records was not order-free in
/// the fences it derived.
///
/// A floor answers the second question and deletes the first. Under the
/// single-use, monotonically-allocated identity contract [`NodeId`] states,
/// every identity a group has ever committed is at or below the greatest one it
/// has committed — so "authorized, plus the greatest identity ever committed" is
/// a complete statement of who may speak and who is retired, with no per-removal
/// history behind it.
///
/// # The rule
///
/// A principal in `peers` is authorized. An identity at or below
/// `retirement_floor` whose principal is **not** in `peers` is **retired**: the
/// cluster committed its removal, and it must never be admitted under that
/// identity again. An identity *above* the floor is simply not authorized yet —
/// a replica whose addition has not committed here, or one this deployment has
/// provisioned and the cluster has not admitted.
///
/// The distinction is worth carrying because the two are not equally repairable.
/// An unauthorized principal becomes authorized by the next publication; a
/// retired one does not, because the floor never falls.
///
/// # What an implementation may assume, and what it may not
///
/// **The floor is monotone.** A driver's floor is the greatest identity it has
/// ever seen in a committed configuration, so a later publication never carries a
/// lower one. An implementation may therefore keep the highest floor it has ever
/// accepted, and doing so is what makes a *stale* policy unable to widen
/// admission: a publication the link layer missed leaves fewer identities
/// retired, never more.
///
/// **The set is authoritative and is re-read on every publication.** This value
/// replaces the previous one whole. An implementation must not latch a denial
/// past the policy that produced it — if a later policy authorizes a principal,
/// that principal is authorized, and a link torn down for a retired peer must be
/// re-establishable.
///
/// That is weaker than the append-only fenced set `fence_peer` allowed, and the
/// weakening is exactly the unbounded per-removal record this design deletes.
/// What replaces it is a property of the *driver*: it never authorizes an
/// identity its own spent test refuses, and that test is monotone for the life of
/// an incarnation. See `PeerControlPlaneCheckpoint` for what carries it across
/// one.
///
/// **The local replica is not a peer of itself**, so it is never in `peers` and
/// this policy says nothing about it. A node does not authorize its own frames,
/// and a driver adopted under a fresh identity retires its previous one here like
/// any other: the old identity is at or below the floor and absent from the set.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PeerPolicy<P> {
    peers: Vec<P>,
    retirement_floor: Option<NodeId>,
}

impl<P> PeerPolicy<P> {
    /// Creates a policy from the authorized principals and the retirement floor.
    ///
    /// `retirement_floor` is `None` before this group has committed any
    /// configuration, which retires nothing. It is not zero: [`NodeId`] `0` is a
    /// legal identity and cannot double as "no floor".
    #[must_use]
    pub fn new(peers: Vec<P>, retirement_floor: Option<NodeId>) -> Self {
        Self {
            peers,
            retirement_floor,
        }
    }

    /// Returns the currently authorized principals.
    #[must_use]
    pub fn peers(&self) -> &[P] {
        &self.peers
    }

    /// Consumes the policy and returns the authorized principals.
    #[must_use]
    pub fn into_peers(self) -> Vec<P> {
        self.peers
    }

    /// Returns the greatest identity this group has ever committed, if any.
    ///
    /// Half of the denial rule; [`PeerPolicy::peers`] is the other half. An
    /// implementation that maps principals to replicas — which every
    /// [`AuthenticatedPeerValidator`] does — refuses an identity at or below this
    /// whose principal the set does not name.
    #[must_use]
    pub fn retirement_floor(&self) -> Option<NodeId> {
        self.retirement_floor
    }
}
